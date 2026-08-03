# Document and prefer minimum privileges sufficient to run

Source and Target accounts need only the rights required to operate: Initial Load, Incremental Capture (Oracle LogMiner and related reads), Delivery, and alignment checks. We document those per engine and support running with that minimum. Superuser/admin may work but must not be the only supported path when narrower grants are enough.

Operator-facing concrete grants (required vs optional / Lab-only) live in the handbook:

- [Source System — Required Privileges](../../handbook/en/source-system.md#required-privileges)
- [Target System — Required Privileges](../../handbook/en/target-system.md#required-privileges-target)
- [Security — Required Privileges pointer](../../handbook/en/security.md#required-privileges-pointer)
