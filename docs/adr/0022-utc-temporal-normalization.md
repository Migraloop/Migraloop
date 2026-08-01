# Temporal values normalize to UTC; naive times use a user-defined timezone

Timezone-aware Oracle values become absolute instants and are stored/processed as UTC in the Platform Store. Timezone-naive DATE/TIMESTAMP values are interpreted with a **user-defined timezone** configured for the Source/Deployment (not inferred from the app host’s local zone), then converted to UTC. MongoDB Delivery writes UTC datetime. Leaving naive timestamps ambiguous or stringifying all times is rejected.
