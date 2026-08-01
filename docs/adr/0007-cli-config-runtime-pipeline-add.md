# Control plane is config + CLI, with runtime Pipeline adds

v1 manages Deployments through declarative YAML/JSON and a CLI (apply/status/pause), plus minimal HTTP health/status if useful. Users must be able to add a Pipeline while the Deployment is running: apply accepts the new Pipeline without restarting the whole process, existing Pipelines continue, and the new Pipeline starts its own Initial Load/Incremental path as required. A full REST/UI control plane can come later on top of the same declarative model.
