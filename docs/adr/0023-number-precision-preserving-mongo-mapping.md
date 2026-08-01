# Oracle NUMBER maps for precision; unsafe columns are resolved at Pipeline config time

NUMBER conversion is driven by Oracle precision/scale into Mongo numeric types that preserve accuracy (integers via integer/Long types; decimals via Decimal128 where they fit). IEEE double is not the default.

If a column’s **declared** precision/scale cannot fit safe Mongo numeric types, the platform detects this when the Pipeline is defined/applied and requires an explicit choice: **remove the field** from the Managed output or **map it to string**. That is not handled by runtime per-row quarantine. Runtime quarantine remains for unexpected apply failures on otherwise accepted fields.
