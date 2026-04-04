//! Build script for the `analyze_errors_size` utility.
//!
//! Parses `scylla/src/errors.rs` with `syn`, collects the names of every
//! top-level `pub` (but NOT `pub(crate)` / `pub(super)` / etc.) `struct` and
//! `enum`, then writes `$OUT_DIR/error_types.rs` — a file that is
//! `include!`-ed by `src/main.rs`.
//!
//! The generated file looks like:
//!
//! ```ignore
//! &[
//!     ("ExecutionError",   std::mem::size_of::<scylla::errors::ExecutionError>()),
//!     ("PrepareError",     std::mem::size_of::<scylla::errors::PrepareError>()),
//!     // …
//! ]
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use syn::visit::Visit;
use syn::{Item, Visibility};

// ---------------------------------------------------------------------------
// Visitor that accumulates names of fully-public top-level structs/enums
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PubTypeCollector {
    names: Vec<String>,
}

impl<'ast> Visit<'ast> for PubTypeCollector {
    // Only visit top-level items; do NOT recurse into modules / impls / etc.
    fn visit_file(&mut self, file: &'ast syn::File) {
        for item in &file.items {
            match item {
                Item::Struct(s) if matches!(s.vis, Visibility::Public(_)) => {
                    self.names.push(s.ident.to_string());
                }
                Item::Enum(e) if matches!(e.vis, Visibility::Public(_)) => {
                    self.names.push(e.ident.to_string());
                }
                _ => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Locate `scylla/src/errors.rs` relative to the workspace root.
///
/// `CARGO_MANIFEST_DIR` is the `utils/analyze_errors_size/` directory;
/// the workspace root is two levels up.
fn errors_rs_path() -> PathBuf {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set by Cargo");
    Path::new(&manifest_dir)
        .parent()
        .expect("analyze_errors_size/ must have a parent directory (utils/)")
        .parent()
        .expect("utils/ must have a parent directory (workspace root)")
        .join("scylla")
        .join("src")
        .join("errors.rs")
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let errors_path = errors_rs_path();

    // Tell Cargo to re-run this script whenever errors.rs changes.
    println!("cargo:rerun-if-changed={}", errors_path.display());

    let source = fs::read_to_string(&errors_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", errors_path.display(), e));

    let file = syn::parse_file(&source)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {}", errors_path.display(), e));

    let mut collector = PubTypeCollector::default();
    collector.visit_file(&file);

    let names = collector.names;
    assert!(
        !names.is_empty(),
        "No public error types found in {}",
        errors_path.display()
    );

    // -----------------------------------------------------------------
    // Code generation
    // -----------------------------------------------------------------
    // We emit a `&[(&str, usize)]` array expression so main.rs can
    // simply `include!` it without any extra boilerplate.

    let mut code = String::from("&[\n");
    for name in &names {
        code.push_str(&format!(
            "    (\"{name}\", ::std::mem::size_of::<::scylla::errors::{name}>()),\n"
        ));
    }
    code.push_str("]\n");

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR must be set by Cargo");
    let out_path = Path::new(&out_dir).join("error_types.rs");

    fs::write(&out_path, &code)
        .unwrap_or_else(|e| panic!("Failed to write {}: {}", out_path.display(), e));
}
