# Control plane is config + CLI with full runtime Pipeline lifecycle

v1 manages Deployments through declarative YAML/JSON and a CLI (apply/status/pause/resume/remove/change), plus minimal HTTP health/status if useful. Runtime operations must not require restarting the whole Deployment.

- **Add**: start the new Pipeline (Initial Load as needed) while others keep running.
- **Pause / resume**: stop or continue Delivery/processing for that Pipeline.
- **Remove**: stop the Pipeline and cease Delivery.
- **Change**: apply a new Pipeline revision—pause old Delivery, rebuild that Pipeline's Derived Dataset and re-Deliver as the change requires, then resume incremental work. Shared Base Datasets are not rebuilt. Metadata-only changes may skip rebuild.

A full REST/UI can come later on the same declarative model.
