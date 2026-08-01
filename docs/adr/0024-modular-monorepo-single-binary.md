# Modular monorepo with a single v1 binary, seams for later paid modules

The codebase is one repository split into modules aligned with the domain (capture, platform-store, transform, delivery, cli, app). v1 still ships as one app binary plus Platform Store. This keeps adoption simple while preserving clear boundaries so future open-core/enterprise modules can be licensed and packaged without extracting a ball-of-mud later. Multi-repo split is deferred until there is a concrete packaging need.
