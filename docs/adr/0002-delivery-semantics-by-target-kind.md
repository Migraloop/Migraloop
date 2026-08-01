# Delivery semantics differ by target kind

Delivery always writes only Managed Columns/fields and may delete an entire document/row when an Output Identity disappears. On document targets (v1: MongoDB), the platform does not catalog non-managed fields—it only writes Managed keys and leaves everything else alone. On relational targets (future), Managed Columns are schema the platform must create/maintain. v1 code only needs document Delivery, but the relational rule is recorded now so later SQL targets do not reinvent ownership boundaries.
