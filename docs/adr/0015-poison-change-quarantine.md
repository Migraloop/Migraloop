# Poison changes are quarantined; Pipelines keep running

Unlike stream-wide blockers (e.g. unblockable DDL), a single repeatedly failing change or Output Identity must not pause the whole Pipeline. After bounded retries the platform quarantines that change/identity, alerts operators, and continues processing. Quarantined keys are explicitly unhealthy/not aligned until repair or retry—never a silent skip. Endless retry that blocks the stream is rejected.
