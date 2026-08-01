# Temporal values normalize to UTC through the platform

Oracle temporal columns are converted with schema-driven rules: timezone-aware timestamps become absolute instants stored/processed as UTC in the Platform Store; DATE and timestamp-without-time-zone follow a single fixed, documented interpretation (not silent local-TZ guessing). MongoDB Delivery writes UTC datetime. Stringifying all times or leaving timezone behavior undefined is rejected for v1.
