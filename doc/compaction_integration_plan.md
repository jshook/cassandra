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
- Builds `FixedBitSet` (all-live for MVP; dead-node computation is a follow-up)
- Builds `OrdinalMapper.OffsetMapper` per source with sequential offsets
- Calls `OnDiskGraphIndexCompactor.compact(outputPath)`
- Writes `POSTING_LISTS` using `V5VectorPostingsWriter` with
  `ZERO_OR_ONE_TO_MANY` structure and an identity mapper
- Writes `PQ` from the first source segment's `ProductQuantization` codebook

**Postings approach:** The caller passes the vector → output-row-IDs
`ChronicleMap` built during the row-by-row pass. Postings lookup at write time:
`ordinal → MergedSourceVectorValues.getVector(ordinal) → postingsMap.get(vector) → rowIds`.

**PQ output note:** The compactor internally retrains PQ; for the first
implementation the source PQ codebook (from `source[0].getPQ()`) is written to
the Cassandra PQ file. A follow-up should expose the retrained codebook from
the compactor.

**MVP limitation — live nodes:** All source nodes are marked live (`FixedBitSet`
all-true). Deleted nodes have no postings in the output and are therefore
invisible to queries, but they consume graph space. Accurate dead-node tracking
requires knowing which source SSTable row IDs survive the compaction; this is
a follow-up (see §Dead Nodes below).

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
- All source graphs expose inline vectors (not NVQ-only)
- All sources use `PRODUCT_QUANTIZATION` compression

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

### Dead node tracking
The MVP marks all source nodes live. To avoid including deleted nodes in the
output graph, we need to build `FixedBitSet` per source by:
1. For each source segment ordinal, looking up its source SSTable row IDs
   via `OnDiskOrdinalsMap`
2. Determining if those rows survived the compaction

Approach A: During the row-by-row pass, track which source-segment row IDs
appear in the output (requires the compaction layer to expose per-source row
origins — currently abstracted away by `CompactionIterator`).

Approach B: Query tombstone data directly per source SSTable
(via `CompactionController`) for each ordinal's rows.

### PQ retraining
The compactor internally retrains PQ on the merged dataset. The retrained
codebook should be written to the Cassandra PQ file instead of the source PQ.
Requires an API on `OnDiskGraphIndexCompactor` to expose the retrained
`ProductQuantization` after `compact()` returns.

### SAI codec header/footer for TERMS_DATA
The merge path writes the raw jvector graph at `termsOffset=0` with no SAI
codec header or footer. SAI checksum validation (`SAICodecUtils.checkFooter()`)
will fail for this component during scrubbing. Normal querying is unaffected.
Fix: add a `startOffset` parameter to `OnDiskGraphIndexCompactor.compact()` (or
use a temp file and copy with SAI header wrapping).

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
