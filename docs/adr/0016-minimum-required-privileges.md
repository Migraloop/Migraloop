# Document and prefer minimum privileges sufficient to run

Source and Target accounts need only the rights required to operate: Initial Load, Incremental Capture (Oracle LogMiner and related reads), Delivery, and alignment checks. We document those per engine and support running with that minimum. Superuser/admin may work but must not be the only supported path when narrower grants are enough.
