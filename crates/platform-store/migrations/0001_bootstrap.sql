-- Bootstrap Platform Store schema. Later slices add Deployment / Pipeline state.

CREATE TABLE platform_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO platform_meta (key, value) VALUES ('bootstrap', '1');
