# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Ngô Hoàng Thụy Khuê/2A202603017

**Repo:** https://github.com/kelsingo/K4-Track02-Day17-NgoHoangThuyKhue-2A202603017-DataPipelineEngineering

**Commit bài nộp:** `b7f5658`

**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** sửa code và file MD report 

**Nguồn tham khảo khác (nếu có):** 

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets`: 24 hàng cho 12 ticket; T-91 trả về 3 hàng (`low/open`, `high/open`, `high/closed/bug`) thay vì 1 hàng `high/closed/bug`. | `gold_feature_daily` lệch full recompute (`c50b8851affe` ≠ `8630e04a61d1`); u05 ngày 08-12 chỉ có (2 events, 1 click, 0 down) thay vì (5, 3, 1). | T-97 vẫn có trong `silver_tickets` (`is_deleted=false`, còn body), trong snapshot `v2026-08-16` (1 hàng) và `gold_doc_chunks` (2 chunk). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup trong một batch rồi `INSERT` thẳng, nên mỗi batch thêm một hàng mới thay vì ghi theo khoá. | `LOOKBACK_DAYS = 0` với giả định "event tới trong vài giây", nhưng đo từ Bronze P99 = 3 ngày: batch 08-15 chứa event ngày 08-12 mà không ai tính lại partition 08-12. | Bản ghi delete có `after = null`, nên `after->>'ticket_id'` là NULL và `WHERE ticket_id IS NOT NULL` loại luôn dòng xoá. Silver không bao giờ biết ticket đã bị xoá. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: thay `INSERT` bằng `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`. Dedup trong batch (`QUALIFY ... _lsn DESC`) giữ nguyên. | `pipeline/config.py`: `LOOKBACK_DAYS = 3` (= ceil(P99)). `build_feature_daily` đã xoá-và-ghi lại `[day-3, day]` theo `event_time`, nên không cần đổi logic. | `pipeline/staging.py`: `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`. Delete thành tombstone (`is_deleted=true`, user_id/subject/body null); Kafka tombstone (`value=null`, `_op` null) vẫn bị bỏ vì không mang thay đổi. |
| **Khái niệm trên slide** | Entity table + MERGE/upsert theo khoá; thứ tự thay đổi bằng LSN; idempotent replay. | Event time vs ingest time; lateness P99 → lookback; overwrite-partition. | CDC delete (`op=d`) vs Kafka tombstone; tombstone/soft delete; xoá lan truyền xuống Gold/RAG. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50=0.00, p95=2.90, max=3, trên 43 record) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f` (C0 = C1 = C2 = C3; feature `8630e04a61d1`, training `9370ca77af23`, chunks `cb9ebd12fdcc`)
- `make verify`: 18/18 ALL PASS; `make test`: 34 passed; dbt `PASS=19`
- `make parity`: PARITY (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể có trạng thái nên cần upsert theo khoá + LSN; feature là phép tổng hợp theo ngày nên xoá-và-tính-lại cả partition là idempotent và tự sửa khi event muộn tới.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ `ticket_id` và `_lsn` để replay batch cũ không hồi sinh ticket đã xoá, trong khi mọi cột cá nhân đã null.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: tái lập được thí nghiệm ML; feedback muộn tạo phiên bản mới thay vì âm thầm đổi dữ liệu đã huấn luyện.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu chỉ vài chục dòng/ngày, chạy một máy trong dưới 2 giây; Spark chỉ thêm chi phí vận hành mà không có lợi về quy mô.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   *Trả lời:* Bất biến áp dụng cho dữ liệu *không đổi ngoài ý muốn*, không phải cho dữ liệu cá nhân bị yêu cầu xoá; quyền xoá (GDPR/NĐ 13) thắng. Snapshot cũ không sửa tại chỗ mà *chu trình hoá*: dựng lại snapshot từ Bronze sau khi đã xoá/che T-97 (tạo phiên bản mới, đánh dấu phiên bản cũ là bị thu hồi và xoá vật lý), ghi lại trong nhật ký vòng đời; model đã huấn luyện trên bản cũ cần được đánh giá/huấn luyện lại. Bronze cũng phải áp dụng xoá/crypto-shredding cho T-97.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   *Trả lời:* Thêm bước nhận diện thực thể (NER tiếng Việt, hoặc LLM với tiền tố tên) ngay khi vào Silver, *cùng* regex, che tên người/địa chỉ/CMND thành `<NAME>`...; thêm cổng kiểm tra trước khi ghi Gold/RAG và trước khi đưa vào snapshot. Đo bằng tập mẫu gán nhãn tay (precision/recall trên PII, ưu tiên recall), tỉ lệ PII còn sót khi lấy mẫu định kỳ, và test hồi quy trong CI (ví dụ "Nguyễn Văn An" không được xuất hiện ở Silver/Gold).


## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                                                                                               [100%]
34 passed in 0.69s
$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /Users/mal/Documents/Vin/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
04:12:31  Running with dbt=1.12.5
04:12:31  Registered adapter: duckdb=1.11.0
04:12:31  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
04:12:31  
04:12:31  Concurrency: 1 threads (target='dev')
04:12:31  
04:12:31  1 of 19 START sql view model main.stg_events ................................... [RUN]
04:12:31  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.03s]
04:12:31  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
04:12:31  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.01s]
04:12:31  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
04:12:31  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.05s]
04:12:31  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
04:12:31  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.05s]
04:12:31  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
04:12:31  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.06s]
04:12:31  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
04:12:31  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
04:12:31  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
04:12:31  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
04:12:31  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
04:12:31  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
04:12:31  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
04:12:31  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.01s]
04:12:31  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
04:12:31  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.05s]
04:12:31  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
04:12:31  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
04:12:31  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
04:12:31  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
04:12:31  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
04:12:31  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
04:12:31  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
04:12:31  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
04:12:31  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
04:12:31  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
04:12:31  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
04:12:31  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
04:12:31  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.01s]
04:12:31  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.01s]
04:12:31  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.01s]
04:12:31  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.01s]
04:12:31  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.01s]
04:12:31  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
04:12:31  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.01s]
04:12:31  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.09s]
04:12:31  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
04:12:31  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
04:12:31  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
04:12:31  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
04:12:31  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
04:12:31  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
04:12:31  
04:12:31  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.51 seconds (0.51s).
04:12:32  
04:12:32  Completed successfully
04:12:32  
04:12:32  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
