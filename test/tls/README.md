# How to use these certificates for smoke tests of PEER verification
Run a scylla instance with `db.crt` and `db.key` as certificate and keyfile.
In your rust program:
Create an openssl's context builder with `let mut context_builder = SslContextBuilder::new(SslMethod::tls())?`.
Create a path to file ex. `let ca_dir = fs::canonicalize(PathBuf::from("./test/certz/ca.crt"))?;`
Add this path to your builder `context_builder.set_ca_file(ca_dir.as_path;`
Set your verify mode to PEER `context_builder.set_verify(SslVerifyMode::PEER);`
Connect to your scylla instance.
