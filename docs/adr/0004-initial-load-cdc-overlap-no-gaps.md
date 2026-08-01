# Initial Load to Incremental Capture must overlap—no cutover gaps

Starting a Pipeline uses Initial Load plus Incremental Capture. The cutover must not leave Base Datasets out of sync with the Source System. We establish a low-watermark capture position (Oracle v1: LogMiner SCN/position), run Initial Load while capture overlaps that window, and dedupe/idempotently apply overlapping changes. Missing changes during cutover are unacceptable; duplicate applies are acceptable. Post-cutover Source Alignment remains a safety net, not the primary way to fix a gapped hand-off.
