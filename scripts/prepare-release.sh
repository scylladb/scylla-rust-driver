#!/bin/sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <release-num>"
    exit 1
fi

new_release=$1
root_dir=$(git rev-parse --show-toplevel)

echo "Preparing release ${new_release}"
echo "Root repo directory: ${root_dir}"

sed -i "s/^version =.*/version = \"${new_release}\"/g" ${root_dir}/docs/pyproject.toml
sed -i "s/^version =.*/version = \"${new_release}\"/g" ${root_dir}/scylla/Cargo.toml

git add ${root_dir}/docs/pyproject.toml ${root_dir}/scylla/Cargo.toml
git commit -m "scylla: bump version to ${new_release}"
git tag v${new_release}

