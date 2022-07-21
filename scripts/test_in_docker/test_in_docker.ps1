if ( -not (docker container ls | Select-String scylla_rust_driver)) {
        echo "Scylla container must be running to execute tests (use 'make scylla-up')."
        exit 1
}

docker image ls -q scylla_rust_driver_testing
if ($LASTEXITCODE -ne 0) {
        docker build -t scylla_rust_driver_testing .
}

if (-not "$CARGO_HOME") {
        $CARGO_REGISTRY="${HOME}/.cargo/registry"
} else {
        $CARGO_REGISTRY="${CARGO_HOME}/registry"
}

$CWD = Get-Location

docker run --name "scylla-rust-driver-testing" `
                --network scylla_rust_driver_public `
                -v "${CWD}/../..:/scylla_driver:Z" `
                -v "${CARGO_REGISTRY}:/usr/local/cargo/registry:z" `
                -it --rm `
                --env CARGO_HOME=/scylla_driver/.cargo_home `
                --env SCYLLA_URI=192.168.100.100:9042 `
                -w /scylla_driver `
                scylla_rust_driver_testing `
                cargo test
