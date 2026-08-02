-- Persist operator-visible Sync lag so status stays coherent across process restart.

ALTER TABLE base_datasets
    ADD COLUMN sync_lag INTEGER NOT NULL DEFAULT 0;
