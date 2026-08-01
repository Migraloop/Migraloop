# Temporal values normalize to UTC; naive times use DB timezone or user override

Timezone-aware Oracle values become absolute instants and are stored/processed as UTC. Timezone-naive DATE/TIMESTAMP values are interpreted with the **Oracle DB timezone when the platform can read it**; if not, the user sets a timezone on the **Source System / Deployment** (single zone for that source). That instant is then stored as UTC. MongoDB Delivery writes UTC datetime. Do not guess from the app host timezone; do not require per-table or per-Pipeline zones in v1.
