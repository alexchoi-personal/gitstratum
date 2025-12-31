# GitStratum Object Cluster Architecture

The gitstratum-object-cluster crate implements a distributed blob storage system designed specifically for storing Git objects such as blobs, trees, and commits. This crate provides the storage layer that handles the actual binary content of a Git repository, separating it from the metadata layer that handles references and commit graphs. The architecture prioritizes high throughput for both read and write operations, transparent compression to reduce storage costs, and reliable replication across multiple nodes for durability.

## Storage Backend

The core storage functionality is provided by the RocksDbStore struct, which wraps the RocksDB embedded key-value database. When a store is opened, it scans all existing entries to initialize counters for the total number of blobs and total bytes stored. The store uses the object identifier as the key and stores compressed blob data as the value. RocksDB is configured with a 64 megabyte write buffer and four parallel background jobs for compaction and flushing operations. The store disables RocksDB's built-in compression because the crate implements its own compression layer, giving more control over the compression algorithm and level.

The compression subsystem provides transparent compression and decompression of blob data before writing to and after reading from RocksDB. The default compression algorithm is Zlib at level 6, which provides a good balance between compression ratio and CPU usage. The CompressionConfig struct allows configuring the compression type and level, and the CompressionType enum supports either Zlib compression or no compression at all. All compression and decompression happens automatically within the store methods, so callers work with uncompressed blob data.

The tiered storage system provides infrastructure for organizing data across hot and cold storage tiers. The TieredStorageConfig specifies thresholds for determining which tier an object belongs to, including a maximum byte size for the hot tier and a number of days after which inactive objects move to cold storage. The TieredStorage struct tracks byte counts for each tier and provides methods to determine which tier an object should be placed in based on its size and last access time. Objects can be moved between tiers using the move_to_cold and promote_to_hot methods, which atomically update the byte counters for both tiers.

## Object Store Interface

The ObjectStore type alias currently points to RocksDbStore and serves as the primary storage interface used throughout the crate. The store provides five core methods for object management. The get method retrieves a blob by its object identifier, returning an optional Blob if found or None if the object does not exist. The put method stores a blob, automatically handling compression and updating internal counters only when a new object is inserted rather than an update. The delete method removes an object and returns whether the object existed before deletion. The has method provides a quick existence check without retrieving the full blob data. The stats method returns a StorageStats struct containing the total blob count, total bytes, used bytes, available bytes, and I/O utilization percentage.

The store also provides iteration methods for bulk operations and replication. The iter method returns an iterator over all stored blobs, useful for backup operations or full replication. The iter_range method filters blobs by their position on the hash ring, returning only those whose position falls within the specified range. This range-based iteration is essential for the replication system, as it allows nodes to stream only the objects they are responsible for based on the consistent hashing scheme.

## gRPC Server

The ObjectServiceImpl struct implements the gRPC service that exposes the object store to the network. It wraps an ObjectStore in an Arc for thread-safe sharing across request handlers. The server provides protocol buffer conversion methods to translate between the protobuf Blob and Oid types and the core crate's Blob and Oid types. These conversions handle the serialization format used for network communication while maintaining type safety.

The get_blob RPC retrieves a single blob by its object identifier, returning a response that indicates whether the blob was found and includes the blob data if present. The get_blobs RPC accepts multiple object identifiers and streams back the corresponding blobs as they are retrieved, allowing efficient batch retrieval without waiting for all blobs to be found before starting the response. The put_blob RPC stores a single blob and returns success or failure, while put_blobs accepts a stream of blobs and returns a summary of successful and failed writes.

The has_blob RPC provides a quick existence check that returns only a boolean, avoiding the overhead of transferring blob data when the caller only needs to know if an object exists. The delete_blob RPC removes an object and indicates whether it existed. The stream_blobs RPC streams blobs within a specified position range on the hash ring, which the replication system uses to transfer objects between nodes during rebalancing or repair operations. The receive_blobs RPC accepts a stream of incoming blobs, typically used by nodes receiving replicated data.

The get_stats RPC returns storage statistics for monitoring purposes, including total blob count, storage utilization, and I/O metrics. All RPC methods include instrumentation for distributed tracing using the tracing crate, with debug-level logs for operation tracking and warning-level logs for errors.

## Cluster Client

The ObjectClusterClient provides a client interface for distributed object operations across a cluster of storage nodes. It maintains a reference to a ConsistentHashRing for routing requests and a connection pool for reusing gRPC connections to nodes. The connection pool uses parking_lot's RwLock for efficient concurrent access, creating new connections on demand and caching them for reuse.

For read operations, the client queries the hash ring to determine which nodes should have a copy of the requested object based on the replication configuration. It then tries each node in order until it finds one that has the object or has tried all nodes. If a node fails to respond or returns an error, the client logs a warning and continues to the next node, providing fault tolerance against individual node failures. For write operations, the client attempts to write to all replica nodes and tracks how many succeed, returning success as long as at least one write completed.

The has method checks for object existence across replica nodes, returning true as soon as any node confirms the object exists. The stats method aggregates statistics from all nodes in the cluster, summing blob counts and byte totals while averaging I/O utilization across responding nodes. The client also provides methods to add and remove nodes from the cluster, with remove_node automatically cleaning up cached connections to removed nodes.

## Caching Layer

The cache module provides multiple caching strategies optimized for different access patterns. The HotObjectsCache maintains an in-memory cache of frequently accessed objects, reducing the need to read from the underlying store for popular objects. The cache is configured with a maximum size in bytes and uses an LRU eviction policy to remove least recently accessed objects when the cache becomes full. Statistics track cache hit and miss rates for monitoring cache effectiveness.

The WriteBuffer accumulates write operations in memory before flushing them to the store in batches. This improves write throughput by reducing the number of individual store operations and allowing the store to batch writes efficiently. The buffer is configured with a maximum size and can be flushed manually or automatically when it reaches capacity.

The Prefetcher anticipates which objects will be needed based on access patterns and preloads them into the cache before they are requested. It uses a RelatedObjectsFinder to determine which objects are likely to be accessed together, such as tree entries that are part of the same directory or commits that are part of the same branch. Prefetching happens in the background to avoid blocking the main request path.

## Delta Compression

The delta module implements delta compression for storing similar objects more efficiently. When two blobs have significant content overlap, storing only the differences between them can dramatically reduce storage requirements. The DeltaComputer generates delta instructions that describe how to transform a base object into a target object, supporting copy instructions that reference ranges of the base object and insert instructions that add new data.

The BaseSelector chooses the best base object for delta compression from a set of candidates. It evaluates candidates based on their size and similarity to the target object, preferring bases that will produce small deltas. The selection algorithm balances compression ratio against the computational cost of computing and applying deltas.

The DeltaCache stores computed deltas in memory for quick access, avoiding the need to recompute deltas for frequently accessed object pairs. The DeltaStorage provides persistent storage for deltas, using the same underlying storage backend as the main object store. The StoredDelta struct represents a persisted delta, including metadata about the base object and the delta instructions.

## Garbage Collection

The garbage collection system uses a mark-and-sweep algorithm to identify and remove unreferenced objects. The MarkPhase walks the object graph starting from root references provided by a RootProvider, marking all reachable objects. The MarkWalker traverses object relationships, following references from commits to trees and from trees to blobs. The StaticRootProvider provides a simple implementation that uses a fixed set of object identifiers as roots.

The SweepPhase removes objects that were not marked as reachable during the mark phase. It iterates through all stored objects and deletes those without marks, reclaiming storage space. The SweepConfig controls how aggressively the sweep operates, including options for dry-run mode that only reports what would be deleted.

The GcScheduler coordinates garbage collection runs, tracking state and providing statistics. It ensures that only one garbage collection runs at a time and manages the lifecycle of mark and sweep phases. The scheduler can be configured to run on a periodic schedule or triggered manually.

## Pack Cache

The pack_cache module provides caching for precomputed pack files that are ready to send to Git clients. When a client requests a fetch or clone, the server needs to generate a pack file containing the requested objects. Computing these pack files involves selecting which objects to include, computing delta compression, and serializing the pack format. Caching completed pack files avoids repeating this computation for common requests.

The PackCache stores pack data keyed by a PackCacheKey that identifies the specific combination of objects included. The cache uses time-to-live based expiration configured through TtlConfig and managed by TtlManager. The HotRepoTracker identifies which repositories receive frequent access, allowing the cache to prioritize keeping pack files for popular repositories.

The PackPrecomputer generates pack files in the background for anticipated requests. It accepts PrecomputeRequest structures describing which objects to include and produces cached pack files that can be served immediately when clients make matching requests.

## Replication

The replication module handles distributing objects across multiple storage nodes for durability and availability. The ReplicationWriter coordinates writing objects to replica nodes based on the consistent hashing configuration. The WriteConfig specifies the replication factor, minimum number of successful writes required, and timeout for write operations. The writer tracks statistics on attempted, successful, and failed writes.

The BatchWriter accumulates multiple blobs before sending them to replica nodes, improving efficiency by reducing the overhead of individual write operations. It integrates with the ReplicationWriter to ensure proper routing and success tracking for batched writes.

The QuorumWriter implements quorum-based replication with configurable consistency levels. The QuorumWriteConfig specifies how many replicas must acknowledge a write before it is considered successful. This allows trading off between write latency and durability guarantees.

The ReplicationReader provides methods for reading from replica nodes with configurable read strategies. The ReadConfig controls how many replicas to query and how to handle disagreements. The ReadStrategy enum specifies whether to read from the primary replica only, read from all replicas and compare results, or read from the fastest responding replica.

The ReplicationRepairer detects and repairs inconsistencies between replicas. It compares object existence and content across replica nodes and copies missing or corrupt objects from healthy replicas. The RepairConfig controls how aggressively repair runs and the RepairTask struct represents a specific repair operation to perform.

## Integrity Verification

The integrity module provides cryptographic verification of object content. The compute_oid and compute_blob_oid functions calculate the expected object identifier from blob content by hashing the data. The verify_oid and verify_blob_oid functions compare a stored object's claimed identifier against its computed identifier, detecting any corruption or tampering.

The IntegrityChecker provides batch verification of multiple objects, tracking statistics on verified objects and detected corruptions. The ShaVerifier handles the low-level SHA-256 hashing operations used for object identification. The VerificationResult enum indicates whether an object passed verification, failed verification, or could not be checked due to missing data.

## Error Handling

The ObjectStoreError enum defines all possible error conditions that can occur during object storage operations. Storage-level errors include RocksDb for underlying database errors, Compression and Decompression for encoding failures, and BlobNotFound for missing objects. Network-level errors include Transport for connection failures, Grpc for RPC errors, and InvalidUri for malformed addresses.

Cluster-level errors include HashRing for consistent hashing failures, NoAvailableNodes when the cluster has no healthy nodes, AllReplicasFailed when writes to all replicas fail, and InsufficientReplicas when fewer than the required number of replicas acknowledge a write. Delta-related errors include DeltaTooLarge, DeltaApplyError, and DeltaDeserializeError for various delta compression failures. The IntegrityError variant reports detected data corruption with details about the expected and computed object identifiers.

The error type implements conversion to tonic::Status for proper gRPC error responses, mapping each error variant to an appropriate gRPC status code. BlobNotFound maps to NOT_FOUND, InvalidOid maps to INVALID_ARGUMENT, and availability errors map to UNAVAILABLE. Other errors map to INTERNAL with the error message included in the status details.

## Thread Safety and Concurrency

All public types in the crate are designed for concurrent access from multiple threads. The ObjectStore uses atomic counters with relaxed ordering for statistics that tolerate slight inconsistency in exchange for performance. The ObjectClusterClient uses parking_lot's RwLock for its connection pool, allowing concurrent reads while serializing writes. The ObjectServiceImpl wraps its store in an Arc for shared ownership across async request handlers.

The gRPC server uses Tokio's async runtime for handling concurrent requests. Streaming RPCs use mpsc channels to decouple the producer generating results from the consumer sending them to the client. Background tasks like prefetching and pack precomputation run on spawned Tokio tasks to avoid blocking request handling.

## Storage Statistics

The StorageStats struct provides a snapshot of storage metrics for monitoring and capacity planning. The total_blobs field counts the number of stored objects, while total_bytes reports the total size of stored data after compression. The used_bytes field reports actually allocated storage, which may differ from total_bytes due to storage overhead. The available_bytes field reports remaining storage capacity, and io_utilization reports the current I/O bandwidth usage as a fraction from zero to one.

Statistics are computed on demand rather than continuously updated for most metrics, though blob count and total bytes are maintained incrementally through atomic counters. Cluster-level statistics aggregate node statistics, summing counts and averaging utilization metrics.
