# TimeSeriesStoreImpl Constants Reference

## 1. `DATA_FILE_PATH`  
- **Type**: `String`  
- **Default**: `"timeseries_data.jsonl"`  
- **Meaning**:  
  The path to the on‑disk JSONL file where incoming data points are appended.  
- **Impact**:  
  - All writes go to this file (via batched flushing).  
  - On startup, existing lines are read from this file to repopulate in‑memory state.  
  - Daily file‑based cleanup prunes out old lines from this file.

---

## 2. `retentionPeriodMs`  
- **Type**: `long`  
- **Default**: `TimeUnit.HOURS.toMillis(24)` (i.e. 86 400 000 ms)  
- **Meaning**:  
  How long (in milliseconds) data is kept before it is considered “expired.”  
- **Impact**:  
  - Inserts older than `(now − retentionPeriodMs)` are rejected.  
  - In‑memory cleanup removes any points older than this threshold.  
  - File‑cleanup also discards older lines when rotating the file daily.

---

## 3. `BATCH_SIZE`  
- **Type**: `int`  
- **Default**: `100`  
- **Meaning**:  
  Number of enqueued `DataPoint` objects that must accumulate before forcing an immediate flush to disk.  
- **Impact**:  
  - Higher ⇒ fewer flushes, bigger I/O bursts, lower per‑point overhead.  
  - Lower ⇒ more frequent flushes, finer durability granularity.

---

## 4. `FLUSH_INTERVAL_MS`  
- **Type**: `int`  
- **Default**: `1000` (1 000 ms = 1 second)  
- **Meaning**:  
  Maximum time to wait before flushing a non‑empty batch, even if `BATCH_SIZE` isn’t reached.  
- **Impact**:  
  - Ensures data is persisted at least every `FLUSH_INTERVAL_MS`.  
  - Balances write latency against throughput.

---

## 5. `CLEANUP_INTERVAL_MS`  
- **Type**: `long`  
- **Default**: `60 000` (60 000 ms = 60 seconds)  
- **Meaning**:  
  How often the in‑memory cleaner (`StoreCleaner`) runs to evict expired points.  
- **Impact**:  
  - A shorter interval => memory is reclaimed sooner, but more CPU overhead.  
  - A longer interval => more stale data resides in memory between runs.

---

## 6. File‑Cleanup Schedule (`FileDataCleaner`)  
- **Runs at**: Daily at _1:00 AM_ local time  
- **Initial Delay Computation**:  
  Calculated by `computeInitialDelay(1, 0)`, which finds the next occurrence of 1:00 AM from startup.  
- **Retention Logic**:  
  - Reads the entire JSONL file.  
  - Writes only lines newer than `(now − retentionPeriodMs)` to a temporary file.  
  - Atomically swaps the temp file back into place.  
- **Impact**:  
  Keeps the on‑disk file from growing indefinitely, ensuring only recent data persists.

---
