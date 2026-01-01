# BucketStore Architecture

BucketStore is a key-value storage engine for content-addressed data. It stores Git objects—immutable blobs identified by their SHA-256 hash. The design optimizes for this specific pattern: write once, read many times, never update in place.

**Note:** BucketStore runs on each Object node. Objects are distributed across nodes via consistent hashing (see Object Cluster Architecture). Within each node, BucketStore uses a separate bucket-based lookup. These are two different mappings:

```
OID → which node?     Uses first 8 bytes as ring position (distributed hash ring)
OID → which bucket?   Uses first 4 bytes % bucket_count (local storage)
```

This document covers the second part—how BucketStore organizes data locally on a single node.

## The Two-File Design

Storage is split into two files with different jobs:

```
┌─────────────────────────────────────────────────────────────────┐
│                         BUCKET FILE                              │
│  A fixed-size index. Tells you where to find each key.          │
│                                                                  │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                    │
│  │Bucket 0│ │Bucket 1│ │Bucket 2│ │   ...  │                    │
│  │  4 KB  │ │  4 KB  │ │  4 KB  │ │        │                    │
│  └────────┘ └────────┘ └────────┘ └────────┘                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         DATA FILES                               │
│  Append-only logs. The actual values live here.                 │
│                                                                  │
│  ┌──────────────┬──────────────┬──────────────┬─────────────    │
│  │   Record 1   │   Record 2   │   Record 3   │    ...          │
│  └──────────────┴──────────────┴──────────────┴─────────────    │
└─────────────────────────────────────────────────────────────────┘
```

**Why split them?** Because they have different access patterns:

- The bucket file is small and randomly accessed. It benefits from being cached in memory.
- Data files are large and append-only. They benefit from sequential writes.

## How Buckets Work

The bucket file is pre-allocated and divided into fixed 4KB buckets. Each bucket holds up to 126 entries. To find which bucket a key belongs to:

```
bucket_id = first_4_bytes_of_key % number_of_buckets
```

That's it—one modulo operation. No tree traversal, no following pointers. The bucket's location in the file is just `bucket_id × 4096` bytes.

Each bucket is self-contained:

```
┌────────────────────────────────┐
│ Header (32 bytes)              │
│   - magic number (sanity check)│
│   - entry count                │
│   - checksum                   │
├────────────────────────────────┤
│ Entry 0 (32 bytes)             │
│ Entry 1 (32 bytes)             │
│ ...                            │
│ Entry 125 (32 bytes)           │
├────────────────────────────────┤
│ Overflow pointer (for future)  │
│ Padding                        │
└────────────────────────────────┘
Total: exactly 4096 bytes
```

Within a bucket, we scan entries linearly. With at most 126 small entries, this fits in a few CPU cache lines and is fast.

## Compact Entries

Each entry is 32 bytes—just enough to locate the data:

```
┌─────────────────────────────────────────────────────────┐
│ oid_suffix (16 bytes)  - last half of the 32-byte key  │
│ file_id    (2 bytes)   - which data file               │
│ offset     (6 bytes)   - position in that file         │
│ size       (3 bytes)   - how many bytes to read        │
│ flags      (1 byte)    - deleted? compressed?          │
│ reserved   (4 bytes)   - future use                    │
└─────────────────────────────────────────────────────────┘
```

**Why only store half the key?** The first half determined which bucket we're in. Within a bucket, two keys matching on the first 16 bytes AND the last 16 bytes but differing in the middle is astronomically unlikely for SHA-256 hashes. Storing only the suffix saves space.

**Why weird field sizes like 6-byte offset?** To pack everything into exactly 32 bytes. A 6-byte offset supports files up to 256 terabytes. A 3-byte size supports records up to 16 megabytes. Good enough for Git objects.

## Data Files

Values are stored in append-only data files. "Append-only" means we only write to the end—never modify existing data. This is safe (no partial overwrites) and fast (sequential I/O).

Each record in a data file:

```
┌────────────────────────────────┐
│ Header (40 bytes)              │
│   - magic number               │
│   - checksum                   │
│   - value length               │
│   - timestamp                  │
├────────────────────────────────┤
│ Full key (32 bytes)            │
├────────────────────────────────┤
│ Value (variable length)        │
├────────────────────────────────┤
│ Padding to 4KB boundary        │
└────────────────────────────────┘
```

**Why 4KB alignment?** It enables direct I/O on Linux—bypassing the operating system's page cache to read straight from disk. This matters for large sequential reads. The padding wastes some space but improves throughput.

**Why store the full key in the record?** For integrity checking. We can always re-hash the value and verify it matches the key. The bucket entry stores only a suffix; the full key lives here.

When a data file reaches its size limit (1GB by default), we start a new one. Old files are never modified.

## The Lookup Path

```
get(key):
    1. Compute bucket_id = key[0..4] % bucket_count

    2. Check bucket cache (LRU)
       - Hit: use cached bucket
       - Miss: read 4KB from bucket file at offset bucket_id × 4096
              add to cache

    3. Scan entries in bucket:
       for entry in bucket.entries[0..count]:
           if entry.suffix == key[16..32]:
               found it!

    4. Read data from data file:
       seek to entry.offset in file entry.file_id
       read entry.size bytes
       verify checksum
       return value
```

**Best case**: Bucket is cached → one disk read (data file only).
**Worst case**: Bucket not cached → two disk reads (bucket + data).

The bucket cache is an **LRU cache** (Least Recently Used)—when it's full, we evict the bucket that hasn't been accessed longest. This keeps frequently-accessed buckets in memory.

## The Write Path

```
put(key, value):
    1. Build record: header + key + value + padding

    2. Append to current data file:
       offset = current_file.append(record)
       if file too big: rotate to new file

    3. Update bucket:
       bucket_id = key[0..4] % bucket_count
       bucket = read_bucket(bucket_id)
       bucket.add_or_update_entry(key_suffix, file_id, offset, size)
       write_bucket(bucket_id, bucket)

    4. Invalidate cached bucket (it changed)
```

Writes touch two places: append to data file (fast, sequential) and update bucket (random 4KB write). The data append is the fast path.

## Deletes and Compaction

Deletes don't remove data immediately. That would require rewriting files. Instead, we just set a flag:

```
delete(key):
    1. Find entry in bucket
    2. Set entry.flags |= DELETED
    3. Write bucket back
    4. Track dead_bytes += entry.size
```

The data still exists in the data file, wasting space. We reclaim it during **compaction**:

```
compact(data_file):
    for each record in data_file:
        if not deleted:
            copy to new_file
            update bucket entry with new location
    delete old_file
```

Compaction rewrites data files, keeping only live records. This runs in the background when dead space exceeds a threshold (e.g., 40% of file is dead).

**Why batch deletes this way?** Because rewriting is expensive. Doing it in bulk is more efficient than shrinking the file on every delete. And it's safe—the old file isn't deleted until the new one is complete.

## The Bucket Cache

Reading 4KB from disk for every lookup would be slow. The bucket cache keeps recently-accessed buckets in memory.

```
┌─────────────────────────────────────────────┐
│              Bucket Cache (LRU)             │
│                                             │
│  Recently used buckets stay in memory.      │
│  When full, evict least-recently-used.      │
│                                             │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐           │
│  │ B42 │ │ B17 │ │ B99 │ │ ... │           │
│  │ MRU │ │     │ │     │ │ LRU │  ← evict  │
│  └─────┘ └─────┘ └─────┘ └─────┘           │
└─────────────────────────────────────────────┘
```

Cache size is configurable. More cache = more buckets in RAM = fewer disk reads. The right size depends on your access patterns.

## What This Is Not

**Not Bitcask.** Bitcask keeps ALL keys in a RAM hash table, enabling single-disk-seek reads. BucketStore keeps only buckets in cache—if a bucket isn't cached, you read it from disk. The tradeoff: less RAM usage, potentially slower lookups for cold data.

**Not an LSM-tree** (like RocksDB or LevelDB). Those use a write-ahead log, an in-memory buffer that flushes to sorted files, and background compaction across levels. BucketStore is simpler: writes go directly to bucket file and data file. No write-ahead log, no levels.

## Why This Design

For Git object storage:

1. **Objects are immutable.** Write-once, read-many. Append-only is perfect.

2. **Keys are uniformly distributed.** SHA-256 hashes spread evenly across buckets without extra hashing.

3. **Access has locality.** Cloning a repo accesses objects from that repo. Those objects hash to a subset of buckets that stay cached.

4. **Individual objects are small-ish.** Mostly under 1MB. The 4KB alignment overhead is acceptable.

5. **Simple operations.** Get, put, delete, iterate. No range queries, no transactions.

The design would be wrong for: frequently-updated data (bucket rewrites), large values (alignment waste), random access across millions of keys (cache thrashing), or complex queries (no indexing beyond the hash).
