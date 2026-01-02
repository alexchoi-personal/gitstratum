# LFS Architecture

Git LFS (Large File Storage) stores large files outside the Git repository. Instead of bloating the repo with multi-gigabyte assets, Git stores small pointer files that reference the actual content on an LFS server.

GitStratum's LFS implementation stores objects in cloud object storage (S3, GCS, Azure Blob) and tracks their locations in the Metadata cluster.

## How LFS Works

When a user adds a large file to a repo configured for LFS:

```
1. Git LFS intercepts the file
2. Computes SHA-256 hash of content
3. Stores a pointer file in Git:

   version https://git-lfs.github.com/spec/v1
   oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b...
   size 157286400

4. Uploads actual content to LFS server
```

On clone/checkout, Git LFS downloads the actual content and replaces pointer files.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FRONTEND                                    │
│                                                                             │
│   SSH ─────► Git protocol (clone, push, fetch)                             │
│   HTTPS ───► Git protocol + LFS API                                        │
│                                                                             │
│   LFS endpoints:                                                           │
│     POST /org/repo.git/info/lfs/objects/batch                              │
│     POST /org/repo.git/info/lfs/verify                                     │
│     GET/PUT handled via signed URLs to object storage                      │
└─────────────────────────────────────────────────────────────────────────────┘
         │                                      │
         │ (where is this LFS object?)          │ (generate signed URL)
         ▼                                      ▼
┌───────────────────┐                  ┌───────────────────────────┐
│     METADATA      │                  │    OBJECT STORAGE         │
│                   │                  │                           │
│  LFS object index │                  │  S3 / GCS / Azure Blob    │
│  - oid → location │                  │                           │
│  - size, status   │                  │  Actual file bytes live   │
│  - backend info   │                  │  here, not in GitStratum  │
└───────────────────┘                  └───────────────────────────┘
```

## Why Cloud Object Storage?

LFS objects differ from Git objects:

| | Git Objects | LFS Objects |
|---|-------------|-------------|
| Size | Usually KB-MB | Often GB |
| Access | Frequent (every clone) | Infrequent (only when needed) |
| Caching | High value | Low value (too large) |
| Count | Millions per repo | Hundreds per repo |

Cloud object storage is ideal for LFS:
- **Designed for large files**: Multipart uploads, streaming downloads
- **Cost-effective**: Pay per GB stored, cheap for infrequent access
- **Scalable**: No capacity planning, infinite storage
- **Durable**: 11 nines durability built-in
- **CDN integration**: Edge caching for global access

## The Batch API

LFS clients don't upload/download directly to GitStratum. Instead, they ask for permission and get URLs:

```
Client                          Frontend                     S3
   │                               │                          │
   │── POST /objects/batch ───────►│                          │
   │   { "operation": "upload",    │                          │
   │     "objects": [              │                          │
   │       { "oid": "abc...",      │                          │
   │         "size": 157286400 }   │                          │
   │     ] }                       │                          │
   │                               │                          │
   │                               │── check auth, rate limit │
   │                               │── generate signed URL ───┤
   │                               │                          │
   │◄── { "objects": [ ───────────│                          │
   │       { "oid": "abc...",      │                          │
   │         "actions": {          │                          │
   │           "upload": {         │                          │
   │             "href": "https://s3.../abc?signature=...",   │
   │             "expires_in": 3600│                          │
   │           }                   │                          │
   │         }                     │                          │
   │       }                       │                          │
   │     ] }                       │                          │
   │                               │                          │
   │── PUT (upload directly) ─────────────────────────────────►│
   │                               │                          │
   │── POST /verify ──────────────►│                          │
   │                               │── verify object exists ──►│
   │                               │── update metadata ───────┤
   │◄── 200 OK ───────────────────│                          │
```

**Why signed URLs?**
- Frontend doesn't proxy large file transfers
- Clients upload/download directly to S3
- Frontend only handles auth and metadata
- Scales to thousands of concurrent large uploads

## Metadata Schema

The Metadata cluster stores LFS object information:

```
LfsObject {
    oid: [u8; 32],           // SHA-256 hash (primary key)
    size: u64,               // Size in bytes
    backend: StorageBackend, // S3, GCS, Azure, etc.
    bucket: String,          // "gitstratum-lfs-prod"
    key: String,             // "objects/ab/cd/abcd1234..."
    status: ObjectStatus,    // Pending, Complete, Failed
    created_at: Timestamp,
    repos: Vec<RepoId>,      // Which repos reference this object
}

enum StorageBackend {
    S3 { region: String, endpoint: Option<String> },
    GCS { project: String },
    Azure { account: String, container: String },
}

enum ObjectStatus {
    Pending,   // Upload started but not verified
    Complete,  // Upload verified, object available
    Failed,    // Upload failed or object missing
}
```

**Content-addressed deduplication**: Same file in multiple repos = one storage object. The `repos` field tracks references for garbage collection.

## Storage Backend Abstraction

```rust
#[async_trait]
pub trait LfsBackend: Send + Sync {
    /// Generate a signed URL for upload
    async fn upload_url(&self, oid: &Oid, size: u64, expires: Duration)
        -> Result<SignedUrl>;

    /// Generate a signed URL for download
    async fn download_url(&self, oid: &Oid, expires: Duration)
        -> Result<SignedUrl>;

    /// Check if object exists and matches expected size
    async fn verify(&self, oid: &Oid, expected_size: u64) -> Result<bool>;

    /// Delete an object (for GC)
    async fn delete(&self, oid: &Oid) -> Result<()>;

    /// Get storage statistics
    async fn stats(&self) -> Result<BackendStats>;
}
```

Implementations for S3, GCS, Azure, and MinIO (for self-hosted).

## Object Key Layout

Objects are stored with a sharded path to avoid directory listing limits:

```
s3://gitstratum-lfs-prod/
  objects/
    ab/
      cd/
        abcd1234567890...  (first 2 bytes as directories)
    12/
      34/
        1234abcd567890...
```

This limits any directory to ~256 entries (one per byte value), avoiding S3's list performance issues.

## Upload Flow

```
1. Client: POST /objects/batch (upload request)

2. Frontend:
   - Authenticate user
   - Check write permission on repo
   - Check rate limits
   - For each object:
     - Check if already exists in metadata → skip
     - Generate signed upload URL
     - Create pending metadata record

3. Client: PUT directly to signed URL

4. Client: POST /verify

5. Frontend:
   - HEAD request to storage to verify size
   - Update metadata status to Complete
   - Add repo to object's reference list
```

## Download Flow

```
1. Client: POST /objects/batch (download request)

2. Frontend:
   - Authenticate user
   - Check read permission on repo
   - For each object:
     - Look up in metadata
     - Generate signed download URL (if exists)
     - Return error (if missing)

3. Client: GET directly from signed URL
```

## SSH + LFS

Git over SSH doesn't directly support LFS HTTP API. The workaround:

```
~/.lfsconfig or .lfsconfig in repo:
  [lfs]
    url = https://gitstratum.example.com/org/repo.git/info/lfs

Git operations: SSH
LFS operations: HTTPS (separate connection)
```

Frontend serves both:
- **Port 22 (SSH)**: Git protocol for clone/push/fetch
- **Port 443 (HTTPS)**: Git protocol + LFS API

Both use the same authentication backend (tokens verified against Metadata).

## Garbage Collection

LFS objects can become orphaned when:
- All repos referencing an object are deleted
- A repo removes an LFS-tracked file from all branches

GC process:

```
lfs_gc():
    for each lfs_object in metadata:
        if object.repos is empty:
            # No repos reference this object
            mark_for_deletion(object)
        else:
            for repo in object.repos:
                if not repo_references_object(repo, object.oid):
                    object.repos.remove(repo)

            if object.repos is empty:
                mark_for_deletion(object)

    # After grace period (e.g., 7 days)
    for object in marked_for_deletion:
        storage.delete(object.oid)
        metadata.delete(object.oid)
```

The grace period prevents deleting objects that are being pushed in a new branch.

## Multi-Region / Multi-Backend

Large organizations might want:
- Objects stored in region closest to users
- Different storage tiers (hot/cold)
- Multiple cloud providers for redundancy

```
LfsObject {
    oid: ...,
    replicas: [
        { backend: S3, region: "us-east-1", bucket: "..." },
        { backend: GCS, region: "europe-west1", bucket: "..." },
    ]
}
```

Frontend picks the best replica based on client location.

## Rate Limiting

LFS uploads can be large and long-running. Rate limiting applies at:

1. **Batch API**: Limit requests per user/repo per minute
2. **Signed URL generation**: Limit concurrent uploads per user
3. **Storage bandwidth**: Cloud provider's limits apply naturally

```
Rate limit response:
{
  "message": "Rate limit exceeded",
  "retry_after": 60
}
```

## Resumable Uploads

For multi-gigabyte files, uploads can fail mid-way. S3 multipart upload handles this:

```
1. Client: POST /objects/batch
2. Frontend: Initiate S3 multipart upload, return upload ID + part URLs
3. Client: Upload parts independently (can retry individual parts)
4. Client: POST /verify
5. Frontend: Complete multipart upload, verify total size
```

The LFS protocol supports this via the `multipart` action type.

## Metrics

Key metrics to track:

- `lfs_batch_requests_total`: Batch API calls by operation (upload/download)
- `lfs_objects_total`: Total objects in storage
- `lfs_storage_bytes`: Total bytes stored
- `lfs_upload_bytes_total`: Bytes uploaded over time
- `lfs_download_bytes_total`: Bytes downloaded over time
- `lfs_signed_url_latency`: Time to generate signed URLs
- `lfs_verify_latency`: Time to verify uploads

## Security Considerations

1. **Signed URL expiration**: Short expiry (1 hour) limits exposure if URL leaks
2. **Object-level permissions**: Check repo permissions before generating URLs
3. **Content verification**: SHA-256 of downloaded content must match OID
4. **Size limits**: Enforce max object size at batch API level
5. **Malware scanning**: Optional integration with scanning service before marking Complete

## Why Not Store in Object Cluster?

We considered storing LFS objects in the Object cluster alongside Git objects. Problems:

1. **Size mismatch**: BucketStore optimized for small objects (KB-MB). LFS objects are GB.
2. **Replication cost**: 3x replication of a 10GB file = 30GB. Cloud storage has built-in durability.
3. **Transfer bottleneck**: Proxying large files through Frontend wastes bandwidth.
4. **Cost**: Self-hosted storage is expensive. Cloud object storage is $0.02/GB/month.

Cloud storage is purpose-built for this workload. Use the right tool for the job.
