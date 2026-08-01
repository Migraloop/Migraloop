# Schema changes are handled by Pipeline impact, not blanket pause

Source DDL is classified against each Pipeline's dependencies. Unaffecting changes do not interrupt Pipelines; schema may catch up later. Affecting but still safely applicable changes continue. Blocking changes (apply cannot progress) produce a warning and **pause** the affected Pipeline(s). Endless retry on unblockable DDL is rejected.
