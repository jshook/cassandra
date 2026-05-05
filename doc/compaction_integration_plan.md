# jvector Graph Index Compaction Integration Plan

## Goal

Move graph-index compaction out of Cassandra and into jvector by delegating to
`OnDiskGraphIndexCompactor`. The SSTable / LSM compaction flow for non-vector index
components is **unchanged**; only the HNSW graph segments are affected.

---

## Architecture Overview

### Current flow (Cassandra builds the graph itself)

```
CompactionIterator (live rows only)
  └─ StorageAttachedIndexWriter.addRow()
       └─ SSTableIndexWriter.addTerm()
            └─ VectorOffHeapSegmentBuilder.addInternalAsync()
                 ├─ CompactionGraph.maybeAddVector()  ← vector ingestion + PQ fine-tune
                 └─ CompactionGraph.addGraphNode()    ← GraphIndexBuilder edge insertion
  ...flush...
  └─ VectorOffHeapSegmentBuilder.flushInternal()
       └─ CompactionGraph.flush()
            └─ OnDiskGraphIndexWriter (writes new graph from scratch)
```

### Target flow (jvector merges existing graphs)

```
CompactionIterator (live rows only)
  └─ StorageAttachedIndexWriter.addRow()
       └─ SSTableIndexWriter.addTerm()
            └─ VectorMergeSegmentBuilder.addInternalAsync()
                 └─ records (vector → output row IDs) in ChronicleMap only
  ...flush...
  └─ VectorMergeSegmentBuilder.flushInternal()
       └─ CompactionGraphMerger.merge()
            ├─ OnDiskGraphIndexCompactor (merges source OnDiskGraphIndex objects)
            └─ V5VectorPostingsWriter (writes ordinal → output row ID postings)
```

### Why two phases, and why is Phase 1 reduced to ChronicleMap recording?

**The two-phase structure is imposed by Cassandra's compaction architecture, not a design
choice specific to the vector index.**

Cassandra's compaction machinery is built around `CompactionIterator`, which is the
authoritative source of truth for which rows survive into the output SSTable.
`CompactionIterator` applies tombstone resolution (expired deletes), TTL expiry, and
deduplication across all input SSTables in one forward pass, emitting only the rows that
should exist in the compacted output. Every index component — whether it is a term index,
a numeric index, or a vector index — must hook into this pass via `addRow()` to observe
each live row as it is emitted. There is no way to determine the final output row set
without running this iterator; the information simply does not exist before it completes.

The flush (`flushInternal`) happens after `CompactionIterator` has finished — only then is
it safe to write index files, because only then is the complete output row set known.

**Phase 1 (row-by-row pass) — what it must do:**

For any vector index implementation, Phase 1 must at minimum capture two facts for each
live output row:
1. Which vector does this row carry?
2. What is its output segment row ID?

In the old (build-from-scratch) flow, Phase 1 did far more than this: it inserted every
vector into an in-memory `GraphIndexBuilder`, computing HNSW edges via distance
comparisons and graph traversals (`CompactionGraph.addGraphNode()`). This is the most
expensive part of graph construction.

In the new (merge) flow, we already have high-quality HNSW graphs on disk from the
previous flush or compaction of each input SSTable. Rebuilding edges from scratch would
throw away that quality and cost. Instead, Phase 1 is reduced to the minimum:
**record `vector → output row ID` in a ChronicleMap and nothing else.** No edges are
computed. No in-memory graph is built.

**Phase 2 (flush) — what it uses that information for:**

`CompactionGraphMerger.merge()` receives the completed ChronicleMap and uses it for two
purposes:

1. **Dead-node detection.** During Phase 1, only vectors belonging to *surviving* rows
   were inserted into the ChronicleMap. A source graph node whose vector is absent from the
   ChronicleMap had all its rows deleted; it is excluded from the merged graph and from the
   postings file.

2. **Postings file construction.** `V5VectorPostingsWriter` iterates the merged graph's
   output ordinals, looks up each node's vector in the ChronicleMap, and writes the
   corresponding output row IDs. The ChronicleMap is the only place that knows which output
   row IDs a given vector maps to; this information was accumulated solely during Phase 1.

**The ChronicleMap is the bridge between the two phases.** It is written during Phase 1
(under `CompactionIterator`'s ownership of row liveness) and read during Phase 2 (under
`flushInternal`'s ownership of file writing). This is why it is an off-heap persistent
map rather than a plain `HashMap`: compaction can involve millions of vectors, and the
ChronicleMap avoids putting all that data on the Java heap across the lifetime of the
compaction job.

---

## Integration Points

### 1. `CassandraDiskAnn.java` — Expose source graph data
**File:** `src/java/org/apache/cassandra/index/sai/disk/vector/CassandraDiskAnn.java`

Add public accessors so the compactor path can read the on-disk graph and its
ordinals map from an existing segment:

```java
public OnDiskGraphIndex getOnDiskGraph()    // cast from ImmutableGraphIndex
public OnDiskOrdinalsMap getOrdinalsMap()   // for ordinal→rowId mapping
public boolean isPqUnitVectors()            // needed when writing PQ to output
```

`getPQ()` already exists on `CassandraDiskAnn`.

---

### 2. `CompactionGraphMerger.java` — New core class
**File:** `src/java/org/apache/cassandra/index/sai/disk/vector/CompactionGraphMerger.java`

A new class that wraps `OnDiskGraphIndexCompactor`. It:

- Accepts `List<SourceSegment>` — each wrapping a `CassandraDiskAnn` and
  the segment's row-ID offset
- **Validates upfront** that all sources have `INLINE_VECTORS` and consistent
  `FUSED_PQ` presence; throws `IllegalStateException` immediately for any other
  feature set so the caller can fall back to the legacy rebuild path
- Builds per-source `FixedBitSet` and `OrdinalMapper.MapMapper` using postings-map
  dead-node detection (see §Dead Nodes below)
- Calls `OnDiskGraphIndexCompactor.compact(outputPath)`
- When `FUSED_PQ` is in use, reads the retrained PQ codebook back from the
  compacted graph file and writes it to the Cassandra PQ component
- Writes `POSTING_LISTS` using `V5VectorPostingsWriter` with
  `ZERO_OR_ONE_TO_MANY` structure and an identity mapper
- Writes `PQ` with `CompressionType.NONE` when sources have no compression

**Postings approach:** The caller passes the vector → output-row-IDs
`ChronicleMap` built during the row-by-row pass. Postings lookup at write time:
`ordinal → MergedSourceVectorValues.getVector(ordinal) → postingsMap.get(vector) → rowIds`.

**Supported source feature sets:**
- `INLINE_VECTORS` only (current Cassandra write path): full dead-node detection,
  PQ component written with `CompressionType.NONE`.
- `INLINE_VECTORS` + `FUSED_PQ` (next release): full dead-node detection,
  retrained PQ codebook extracted from compacted graph and written to PQ component.

Any other feature set (NVQ, separated vectors) causes `merge()` to throw
`IllegalStateException` before touching the compactor.

---

### 3. `SegmentBuilder.VectorMergeSegmentBuilder` — New builder variant
**File:** `src/java/org/apache/cassandra/index/sai/disk/v1/SegmentBuilder.java`

New static inner class `VectorMergeSegmentBuilder` alongside the existing
`VectorOffHeapSegmentBuilder`. It:

- Constructor: takes `List<CompactionGraphMerger.SourceSegment>` for the
  merge, plus standard builder args
- Allocates a `ChronicleMap<VectorFloat<?>, CompactionVectorPostings>` (like
  `CompactionGraph`) to track `vector → output row IDs` during the row-by-row pass
- `addInternalAsync()`: records each incoming vector and its output segment row
  ID into the ChronicleMap; **does not build graph edges**
- `flushInternal()`: calls `CompactionGraphMerger.merge()` passing the
  ChronicleMap and metadata
- `requiresFlush()`: returns `false` — the merge path writes one output segment
  regardless of size; if memory pressure forces an early flush, the second
  segment falls back to `VectorOffHeapSegmentBuilder`

---

### 4. `SSTableIndexWriter.newSegmentBuilder()` — Decision point
**File:** `src/java/org/apache/cassandra/index/sai/disk/v1/SSTableIndexWriter.java`

Add a third branch to the vector builder selection (lines 381–398):

```
if (mergePathApplicable && segments.isEmpty() && !termsFileHasContent)
    → VectorMergeSegmentBuilder (new merge path)
else if (pqi != null && V3OnDiskFormat.ENABLE_LTM_CONSTRUCTION)
    → VectorOffHeapSegmentBuilder (existing fine-tune path)
else
    → VectorOnHeapSegmentBuilder (build from scratch)
```

**`mergePathApplicable` conditions:**
- `inputSSTables != null` (compaction operation, not flush)
- At least 2 source graph segments exist across input SSTables
- V5 format is in use (`V5OnDiskFormat.writeV5VectorPostings` returns true)
- **All** source graphs have `INLINE_VECTORS` — if any source lacks it (e.g. NVQ
  sources have `NVQ_VECTORS` only), `collectMergeSources()` returns `null` and
  the whole job falls back to the rebuild path

**Why NVQ sources fall back to rebuild:** `OnDiskGraphIndexCompactor.validateFeatures()`
hard-requires the `INLINE_VECTORS` feature on every source graph and throws
`IllegalArgumentException` if it is absent. NVQ graphs are written with
`NVQ_VECTORS` only (no `INLINE_VECTORS`), so passing them to the compactor
would crash. `CompactionGraphMerger.merge()` also validates this upfront and
throws `IllegalStateException` — but the check in `collectMergeSources()` catches
it earlier so the fallback is clean.

Only inline-vector source graphs are handled by the merge path:
- **`INLINE_VECTORS` only** (current Cassandra write path): dead-node detection
  reads the full-precision vector per node and checks `postingsMap.containsKey(vec)`.
  PQ component written with `CompressionType.NONE`.
- **`INLINE_VECTORS` + `FUSED_PQ`** (next release): same dead-node detection.
  Retrained PQ codebook extracted from compacted graph and written to PQ component.

`termsFileHasContent` guards against the compactor overwriting an earlier segment
if the merge path is invoked for a second segment (which shouldn't happen given
`requiresFlush()=false`, but is a safety check).

---

### 5. `SSTableIndexWriter` — Thread input SSTables through
**File:** `src/java/org/apache/cassandra/index/sai/disk/v1/SSTableIndexWriter.java`

Add `@Nullable Set<SSTableReader> inputSSTables` as an optional constructor
parameter (null for flush/rebuild, non-null for compaction). This is used inside
`newSegmentBuilder()` to filter `indexContext.getView().getIndexes()` to only the
SSTables being compacted.

---

### 6. `V1OnDiskFormat.newPerIndexWriter()` — Extract input SSTables
**File:** `src/java/org/apache/cassandra/index/sai/disk/v1/V1OnDiskFormat.java`

When creating `SSTableIndexWriter` for a compaction operation, cast
`LifecycleNewTracker` to `LifecycleTransaction` (safe for compaction) and call
`originals()` to get the input SSTables:

```java
Set<SSTableReader> inputSSTables = (tracker instanceof LifecycleTransaction)
    ? ((LifecycleTransaction) tracker).originals()
    : null;
return new SSTableIndexWriter(perIndexComponents, limiter, ..., inputSSTables);
```

---

### 7. `V5VectorPostingsWriter` — Null-safe postings lookup
**File:** `src/java/org/apache/cassandra/index/sai/disk/v5/V5VectorPostingsWriter.java`

In `writeGenericOrdinalToRowIdMapping()` and `writeGenericRowIdMapping()`, add
null handling for the `postingsMap.get(vector)` call. For the merge path, source
graph nodes whose rows were all deleted have no entry in the postingsMap; they
should produce an empty posting list (the node exists in the graph but is
invisible to queries).

---

## Implementation Order

1. `CassandraDiskAnn.java` — add accessors (prerequisite for all else)
2. `CompactionGraphMerger.java` — new class
3. `VectorMergeSegmentBuilder` — add to `SegmentBuilder.java`
4. `SSTableIndexWriter.java` — thread input SSTables, add merge path branch
5. `V1OnDiskFormat.java` — extract originals from `LifecycleTransaction`
6. `V5VectorPostingsWriter.java` — null-safe postings

---

## Known Limitations / Follow-ups

### Dead node tracking — inline-vector sources
**Implemented.** For sources that store full-precision vectors on disk
(`INLINE_VECTORS` / `SEPARATED_VECTORS`), dead-node detection is done in
`CompactionGraphMerger.merge()`: each source graph node's full-precision
vector is read from the view and checked against `postingsMap`. Absent means
all of that node's rows were deleted; the node is excluded from the output
graph entirely (not included in the `FixedBitSet` or the global ordinal
assignment).

The ChronicleMap built during Phase 1 serves as the liveness oracle — it
already records exactly which vectors belong to surviving rows, so no
additional pass or `OnDiskOrdinalsMap` lookup is needed.

### Dead node tracking — NVQ sources
**Not applicable — NVQ sources fall back to the legacy rebuild path.**
`OnDiskGraphIndexCompactor` hard-requires `INLINE_VECTORS` on every source;
NVQ graphs (`NVQ_VECTORS` only, no `INLINE_VECTORS`) cannot be passed to the
compactor. `CompactionGraphMerger.merge()` validates this upfront and throws
`IllegalStateException`, and `collectMergeSources()` catches this earlier to
trigger a clean fallback to `VectorOffHeapSegmentBuilder`. No ghost-node
accumulation occurs because NVQ compactions always take the rebuild path.

### PQ retraining
**Implemented.** When sources have `FUSED_PQ`, the compactor retrains the PQ
codebook and embeds it in the output graph file. After `compact()` returns,
`CompactionGraphMerger.merge()` opens the compacted graph with
`ReaderSupplierFactory`, reads the `FusedPQ` feature's `ProductQuantization`,
and writes that retrained codebook to the Cassandra PQ component. This ensures
query encoding at search time uses the same codebook as the in-graph compressed
neighbor scores. No jvector API change was required — the codebook is readable
directly from the written graph file.

### SAI codec header/footer for TERMS_DATA
**Implemented.** The merge path now writes the raw jvector graph to a sibling
temp file first, then copies those bytes through an `IndexOutputWriter` so the
SAI CRC accumulates correctly. `SAICodecUtils.writeHeader()` and
`SAICodecUtils.writeFooter()` wrap the raw graph bytes. `termsOffset` is set to
`SAICodecUtils.headerSize()` (7 bytes) and `termsLength` is the raw graph size,
matching the metadata conventions of the legacy flush path. Scrub validation
will now pass for this component.

### Feature flag
**Implemented.** The merge path is guarded by a runtime-configurable flag.

#### Property

| Property | Default | Description |
|----------|---------|-------------|
| `cassandra.sai.vector.graph_compaction_merge_enabled` | `true` | When `false`, all vector index compactions fall back to the legacy graph-rebuild path. |

#### Where it lives

- **`CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED`** —
  typed enum entry that owns the property name and default value.
- **`CompactionGraphMerger.ENABLED`** — `public static volatile boolean` initialized
  from the property at class-load time. Being `volatile` allows the flag to be
  flipped without a JVM restart (e.g. via a JMX diagnostic endpoint or a test
  helper that writes directly to the field).

The flag is checked as the *first* condition in
`SSTableIndexWriter.newSegmentBuilder()`, so a disabled flag short-circuits
immediately without touching the index view or iterating over source segments.

#### Disabling at startup

Pass the JVM system property before the node starts:

```
-Dcassandra.sai.vector.graph_compaction_merge_enabled=false
```

#### Disabling at runtime (without restart)

From any code path with access to the class (e.g. a JMX bean or a CNDB
diagnostic tool):

```java
CompactionGraphMerger.ENABLED = false;
```

This takes effect on the **next** compaction that picks a segment builder; any
compaction already in progress continues on whatever path it selected.

#### Re-enabling

Set the property back to `true` at startup, or set `CompactionGraphMerger.ENABLED = true`
at runtime. The next eligible compaction will use the merge path again.
