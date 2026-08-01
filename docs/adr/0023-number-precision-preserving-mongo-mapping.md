# Oracle NUMBER maps for precision; overflow may use string by user choice

NUMBER conversion is driven by Oracle precision/scale into Mongo numeric types that preserve accuracy (integers via appropriate integer/Long types; decimals via Decimal128 where they fit). IEEE double is not the default. When a value is too long/precise to fit safe Mongo numeric types, the user can choose to store it as string or to refuse/quarantine—silent lossy coercion is rejected.
