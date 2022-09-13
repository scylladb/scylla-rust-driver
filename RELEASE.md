# Release procedure

The following list includes information how to release version vX.Y.Z of scylla-rust-driver

0. Be a maintainer of this project with write rights to this repository and crates.io/crates/scylla
1. Check out current `main`, e.g. `git fetch; git checkout origin/main`
2. Run `./scripts/prepare-release.sh X.Y.Z`
    this script will prepare a release commit with the version bumped in appropriate places and also create a local vX.Y.Z git tag;
    feel free to verify the commit with `git log -p -1` after the script was executed successfully
3. Push the commit to scylladb/scylla-rust-driver `main` branch, e.g. `git push origin HEAD:refs/heads/main`
4. Push the newly created tag to scylladb/scylla-rust-driver, e.g. `git push origin vX.Y.Z`
5. Write release notes. You can find an example here: https://groups.google.com/g/scylladb-users/c/uOfmdTeq6qM or here: https://github.com/scylladb/scylla-rust-driver/releases/tag/v0.5.0
6. Go to https://github.com/scylladb/scylla-rust-driver/releases , click the `Draft new release` button and follow the procedure to create a new release on GitHub. Use the release notes as its description.
7. Publish the crate(s) on crates.io. For `scylla` crate, go directly to the crate directory, i.e. `/your/repo/path/scylla-rust-driver/scylla`, and run `cargo publish`. It will ask you for an access token, follow the instructions if you haven't set one up.
8. Send the release notes to scylladb-users (https://groups.google.com/g/scylladb-users) as well as Cassandra users (https://lists.apache.org/list.html?user@cassandra.apache.org) mailing lists.

You're done!


