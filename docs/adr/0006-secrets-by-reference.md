# Secrets are supplied by reference, not stored in plaintext

Source, Target, and Platform Store credentials must not live as plaintext in Pipeline definitions or Platform Store rows. v1 accepts secrets from environment variables, Docker secrets, or mounted secret files, referenced by name from configuration. External secret managers (Vault/cloud KMS) can be added later; they are not required to ship a production-safe v1.
