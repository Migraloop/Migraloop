# TLS is supported for all connections and recommended in production

v1 supports TLS for Source System, Target System, and Platform Store connections. Documentation recommends enabling TLS in production. Cleartext remains allowed for local/dev or explicitly chosen environments; we do not hard-fail every non-TLS connection in v1.
