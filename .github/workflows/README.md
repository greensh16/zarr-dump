# GitHub Actions CI Workflows

This directory contains GitHub Actions workflows for continuous integration and deployment of the `zarr-dump` project.

## Workflows

### `ci.yml` - Main CI Pipeline

This is the primary CI workflow that runs on every push and pull request to `main` and `develop` branches. It includes:

- **Code Formatting Check**: `cargo fmt --check` ensures code follows Rust formatting standards
- **Linting**: `cargo clippy -- -D warnings` catches common mistakes and enforces best practices
- **Testing**: `cargo test --all-targets` runs all unit tests, integration tests, and doc tests
- **Integration Tests**: `cargo test --test cli` specifically runs the CLI integration tests that create sample Zarr stores

The integration tests in `tests/cli.rs` create their own sample Zarr stores dynamically, so no external sample data needs to be downloaded. The tests cover:

- Hierarchical Zarr stores with multiple variables
- Consolidated metadata stores
- Error handling for invalid stores
- CLI argument parsing and output formatting
- Dimension inference logic

### `extended-ci.yml` - Extended CI Pipeline

This workflow provides additional checks and runs on pushes to `main` and scheduled weekly runs:

- **Cross-platform Testing**: Tests on Ubuntu, Windows, and macOS with stable and beta Rust
- **Code Coverage**: Generates coverage reports using `cargo-llvm-cov` and uploads to Codecov
- **Release Builds**: Creates optimized release builds for all platforms
- **Security Audits**: Uses `cargo-audit` to check for known vulnerabilities in dependencies
- **External Store Testing**: Creates and tests with larger, more complex sample stores

## Sample Store Creation

The CI workflows handle sample store creation in two ways:

1. **Unit/Integration Tests** (`tests/cli.rs`): Creates lightweight sample stores with:
   - Multiple variables with different dimensions
   - Various compression algorithms (zlib, blosc, zstd)
   - Consolidated and hierarchical metadata formats
   - Edge cases like unlimited dimensions

2. **External Store Testing** (extended CI): Creates larger sample stores to test performance with:
   - Larger array shapes (1000×500×250)
   - More complex compression settings
   - Realistic metadata structures

## Triggers

- **Main CI**: Runs on all pushes/PRs to `main` and `develop`
- **Extended CI**: Runs on pushes to `main`, PRs to `main`, and weekly schedules
- **Security Audits**: Run weekly via cron schedule

## Environment Variables

- `CARGO_TERM_COLOR=always`: Ensures colored output in CI logs
- `RUST_BACKTRACE=1`: Provides detailed error traces for integration tests

## Caching

Both workflows use `Swatinem/rust-cache@v2` to cache Cargo dependencies and build artifacts, significantly reducing CI runtime.

## Artifacts

The extended CI workflow uploads release binaries for all platforms as GitHub Actions artifacts, which can be downloaded and used for testing or distribution.
