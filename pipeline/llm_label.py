"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Validate the model's answer against the schema {"label": <allowed>}.
    Returns the label, or None if it is not valid (-> quarantine, never Gold)."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict) or set(obj) != {"label"}:      # no missing / extra keys
        return None
    label = obj["label"]
    return label if isinstance(label, str) and label in ALLOWED_LABELS else None


def cache_key(text: str, model: str, prompt_version: str) -> str:
    """Rule 1: key = hash(input) + model + prompt version."""
    return hashlib.sha256(f"{model}\x1f{prompt_version}\x1f{text}".encode("utf-8")).hexdigest()


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cached, validated, versioned LLM labelling (the four rules of the slide).

    * cache  : llm_label_cache keyed by hash(text)+model+prompt_version. Every raw
               answer (valid OR invalid) is cached, so a re-run makes 0 LLM calls;
               a new prompt version / model changes the key and re-labels on purpose.
    * schema : answers are validated with parse_label; invalid ones go to
               llm_label_quarantine with a reason and never reach gold_ticket_labels.
    * version: model + prompt_version are stored on every Gold row.
    Gold and quarantine are rebuilt from the cache each run (idempotent; deleted
    tickets disappear because only live Silver tickets are labelled).
    """
    model, prompt_version = llm.model, PROMPT_VERSION     # read at call time
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        cache_key VARCHAR PRIMARY KEY, model VARCHAR, prompt_version VARCHAR,
        raw VARCHAR, label VARCHAR)""")           # label NULL = answer failed validation
    live = live_tickets(con)
    keys = {tid: cache_key(text, model, prompt_version) for tid, text in live}
    cached = {k for (k,) in con.execute("SELECT cache_key FROM llm_label_cache").fetchall()}

    new_rows = {}
    for ticket_id, text in live:
        key = keys[ticket_id]
        if key in cached or key in new_rows:
            continue                                   # cache hit -> no LLM call
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        new_rows[key] = (key, model, prompt_version, raw, parse_label(raw))
    if new_rows:
        con.executemany("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?)", list(new_rows.values()))

    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    con.execute("""CREATE OR REPLACE TABLE llm_label_quarantine (
        ticket_id VARCHAR, raw VARCHAR, reason VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    good, bad = [], []
    for ticket_id, _ in live:
        key = keys[ticket_id]
        raw, label = con.execute(
            "SELECT raw, label FROM llm_label_cache WHERE cache_key = ?", [key]).fetchone()
        if label is None:
            bad.append((ticket_id, raw, "off-schema answer: expected {\"label\": bug|billing|other}",
                        model, prompt_version))
        else:
            good.append((ticket_id, label, model, prompt_version))
    if good:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", good)
    if bad:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?)", bad)
    return {"labeled": len(good), "quarantined": len(bad), "calls": llm.calls,
            "new_calls": len(new_rows)}