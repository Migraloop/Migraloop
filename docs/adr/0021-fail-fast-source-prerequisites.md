# Source prerequisites are documented and checked before run

For Oracle LogMiner (and later capture mechanisms), required Source settings—such as supplemental logging and sufficient redo retention—are documented and validated before a Deployment/Pipeline runs. Unmet prerequisites produce a clear fail-fast error rather than silent incorrect capture. v1 does not automatically alter customer Oracle configuration to satisfy prerequisites.
