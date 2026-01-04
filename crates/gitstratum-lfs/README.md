# gitstratum-lfs

**Status: Work In Progress**

Git LFS support for GitStratum with cloud object storage backends.

## Current State

This crate provides:
- Local filesystem backend (fully implemented)
- LFS batch API types and metadata structures
- Backend trait for implementing storage providers

## Not Yet Implemented

The following cloud storage backends are planned but not yet implemented:
- Amazon S3 (`s3` feature)
- Google Cloud Storage (`gcs` feature)

Cloud SDK dependencies are commented out in Cargo.toml until implementation is complete.
