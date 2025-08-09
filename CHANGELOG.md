# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive GitHub Actions CI/CD workflows for automated testing and releases
- Multi-platform release builds supporting Linux (x86_64), Windows (x86_64), and macOS (x86_64/ARM64)  
- ARM support for macOS Apple Silicon (aarch64-apple-darwin)
- Automated crate publishing to crates.io for stable releases
- Release artifacts with proper binary naming and archiving
- Workflow caching for faster CI builds using `Swatinem/rust-cache@v2`

### Changed
- **BREAKING**: Modernized GitHub Actions workflows using current best practices
- Updated to use `actions/checkout@v4` and `dtolnay/rust-toolchain@stable`
- Improved release workflow with proper permissions and matrix build strategy
- Simplified release workflow configuration by removing problematic cross-compilation targets
- Enhanced release notes with installation instructions and platform-specific downloads

### Removed
- Deprecated GitHub Actions (upgraded to modern versions)
- Removed problematic musl and ARM64 Linux cross-compilation targets that were causing build failures
- Removed legacy workflow configurations

### Fixed
- Fixed repository URLs in Cargo.toml metadata (corrected naming inconsistencies)
- Corrected release workflow permissions and tag reference handling
- Fixed binary stripping for cross-platform builds
- Resolved cross-compilation issues for ARM64 targets

## [0.1.0] - 2024-08-09

### Added
- Initial release of zarr-dump CLI tool
- Core functionality for inspecting and summarizing Zarr store metadata
- NetCDF-style output formatting
- Support for hierarchical and consolidated metadata stores
- Comprehensive CLI integration tests
- MIT license and documentation

### Dependencies
- clap 4.x for command-line argument parsing
- zarrs 0.16 for Zarr store access
- serde/serde_json for JSON handling
- tokio runtime for async operations
- colored output for enhanced readability
