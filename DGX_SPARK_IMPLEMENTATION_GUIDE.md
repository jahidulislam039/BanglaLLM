# SHUDDHIKARAN Implementation Guide for NVIDIA DGX Spark

**Target system:** NVIDIA DGX Spark  
**Memory:** 128 GB unified CPU/GPU memory  
**Local storage:** 4 TB NVMe  
**CPU assumption:** 20 Arm cores  
**Purpose:** Execute the SHUDDHIKARAN Bengali corpus pipeline safely on a single workstation

---

## 1. Hardware-aware design constraints

DGX Spark's 128 GB is unified memory shared by CPU and GPU workloads. It must not be treated as 128 GB of freely available Python heap. The operating system, filesystem cache, GPU models, Arrow buffers, native libraries, and worker processes all draw from the same pool.

Use these operating limits unless measurement supports a change:

| Resource | Normal ceiling | Hard warning |
|---|---:|---:|
| Total used memory | 95 GB | 108 GB |
| CPU-stage worker memory | 80–90 GB aggregate | 100 GB |
| GPU inference model + runtime | 50–70 GB | 80 GB |
| In-flight Arrow batch | 256–512 MB/worker | 1 GB/worker |
| NVMe normal usage | 3.0 TB | 3.2 TB |
| NVMe emergency stop | — | 3.4 TB |
| Free-space reserve | 800 GB preferred | Never below 600 GB |

Do not run a memory-heavy GPU classifier concurrently with document MinHash generation. Unified memory avoids explicit transfers but does not eliminate capacity contention or paging penalties.

---

## 2. Recommended software architecture

Use a Python 3.11 or 3.12 pipeline with:

- PyArrow and Parquet/Zstandard for canonical storage;
- Polars or DuckDB for scans, joins, groupings, and external sorting;
- a compiled or vectorized MinHash implementation;
- memory-mapped NumPy arrays for signatures;
- SQLite or DuckDB only for compact run metadata, not full text;
- Hugging Face tokenizers/SentencePiece for tokenizer experiments;
- fastText or an equivalent small language identifier;
- KenLM only as an optional scoring feature;
- psutil and NVML bindings for resource telemetry.

Avoid:

- one Python object per document at corpus scale;
- pandas for full-corpus operations;
- a global `datasketch.MinHashLSH` object;
- gzipped JSONL as the primary intermediate format;
- multiprocessing pools that copy large objects into each worker;
- storing raw text repeatedly at every stage.

---

## 3. Filesystem layout

Use one immutable raw zone, one stage workspace, and one release zone:

```text
/data/shuddhikaran/
├── raw/                         # Immutable source snapshots
├── registry/                    # Source, license, benchmark manifests
├── runs/
│   └── 2026-07-30_v3/
│       ├── stage_00_ingest/
│       ├── stage_01_normalize/
│       ├── stage_02_doc_dedup/
│       ├── stage_03_quality/
│       ├── stage_05_chunk/
│       ├── stage_07_chunk_dedup/
│       ├── stage_08_policy/
│       ├── stage_09_decontam/
│       ├── stage_10_tokenizer/
│       ├── logs/
│       └── tmp/
├── cache/                       # Download/model cache with size limits
├── releases/                    # Validated immutable releases
└── backups/                     # Manifests/configs; corpus backup may be external
```

Mount or configure temporary directories under the fast NVMe, not a small root partition. Keep model caches bounded and remove superseded checkpoints after validation.

---

## 4. Storage budget for 4 TB NVMe

A safe initial budget is:

| Category | Budget |
|---|---:|
| Immutable compressed raw data | 700 GB |
| Current stage input | 700 GB |
| Current stage output | 700 GB |
| Dedup signatures, bands, pairs, sort spill | 600 GB |
| Models, caches, tokenizer artifacts | 150 GB |
| Logs, metrics, samples, manifests | 50 GB |
| Free-space and failure reserve | 1.1 TB |

Actual allocation depends on source compression and normalized-text expansion. Measure it during the benchmark phase.

### Cleanup rule

A prior stage may be deleted only if:

1. its successor has `_SUCCESS`;
2. all successor shard checksums pass;
3. counts reconcile;
4. audit samples pass;
5. the source/raw input or an external backup can reproduce it;
6. manifests and configs are copied to the release metadata store.

Use a rolling two-stage window: raw data, current input, current output, and temporary workspace. Do not retain every full intermediate on the internal NVMe.

---

## 5. Parquet and sharding configuration

### Recommended defaults

```yaml
storage:
  format: parquet
  compression: zstd
  compression_level: 6
  target_file_size_mb: 512
  row_group_size_mb: 128
  dictionary_encode_metadata: true
  dictionary_encode_text: false
  write_statistics: true
```

Target 256–768 MB compressed files. Tiny files harm metadata and scan performance; multi-gigabyte files make retries expensive.

### Shard identity

Derive output shard identity from:

```text
hash(stage_version, source_id, input_shard_id, config_hash)
```

Write to `*.partial`, call `fsync`, validate, and atomically rename. A resumed run skips only checksum-valid completed shards.

---

## 6. Concurrency strategy

The CPU has 20 Arm cores. More processes are not automatically faster because parsing, compression, hashing, and NVMe traffic compete.

### Starting worker counts

| Workload | Initial workers | Notes |
|---|---:|---|
| Decompression + parsing | 8 | Increase only if CPU is underutilized |
| Unicode normalization | 12 | Usually CPU-bound and lightweight |
| fastText language ID | 8–12 | Batch records per worker |
| MinHash signature creation | 12 | Use compiled/vectorized operations |
| External sort/group | 8 threads | Allow DuckDB/Polars to manage memory |
| GPU classifier inference | 1 process | Batch dynamically; reserve CPU workers for feeding |
| Parquet writing | 4–6 | Avoid excessive concurrent compression |
| SentencePiece training | 1 process | Use a sampled corpus |

Set explicit thread limits for OpenMP, BLAS, Arrow, DuckDB, tokenizers, and each worker. Otherwise nested thread pools can oversubscribe the 20 cores.

### Backpressure

Use bounded queues. A producer must pause when the next stage or writer is slower. Queue capacity should represent minutes of work, not millions of documents.

---

## 7. Memory management

### Process model

Prefer independent shard tasks over a persistent global multiprocessing pool. Workers open their own input shard, emit bounded batches, close resources, and exit. This limits fragmentation and simplifies retries.

### Memory rules

- Scan only required columns.
- Keep text in Arrow arrays rather than Python strings where practical.
- Use iterators and record batches.
- Memory-map signature matrices.
- Flush output at bounded intervals.
- Never accumulate all candidate pairs in memory.
- Disable or cap unbounded library caches.
- Record both resident memory and system-wide unified-memory use.

### Adaptive scheduler

Pause task launch above 95 GB used memory. Stop and checkpoint above 108 GB or if sustained swap/paging is detected. Resume only after memory returns below a safe hysteresis threshold such as 85 GB.

---

## 8. Stage-by-stage implementation

## 8.1 Stage −1: inventory and benchmark

Before the full run, sample at least 1% from every major source and no fewer than one large shard per format. The benchmark should include at least one million documents, preferably five million.

Capture:

```text
records/s
compressed MB/s
uncompressed MB/s
peak memory
output/input byte ratio
MinHash signatures/s
LSH candidates/document
GPU tokens/s
Parquet compression ratio
temporary disk multiplier
```

Calculate estimates as ranges and include 30–50% contingency. Do not publish fixed full-run durations before this benchmark.

---

## 8.2 Stage 0: ingestion

### Execution

1. Read compressed input streams in 64–256 MB logical batches.
2. Convert each source record to the canonical schema.
3. Generate deterministic IDs.
4. Write source-partitioned Parquet.
5. Emit malformed records separately.
6. Validate record reconciliation and checksums.

### Performance target

The stage should be bounded by decompression and parsing, not Python object allocation. Profile one reader at a time because XML, WARC/WET, JSONL, and dataset-library sources have different bottlenecks.

---

## 8.3 Stage 1: normalization and language routing

Perform transformations in fused passes where possible:

1. decode and repair;
2. normalize;
3. compute script statistics;
4. perform batched language inference;
5. route and write.

Do not save a full intermediate between each substep. Store transformation flags in the output schema.

For GPU language or quality models, execute a separate run after CPU normalization. Keep a bounded input prefetch and use dynamic token-based batches rather than fixed document counts.

---

## 8.4 Stage 2: scalable exact and fuzzy deduplication

This is the most important architecture change.

### Exact dedup

Create a narrow table:

```text
content_id | doc_id | source_id | canonical_priority | token_count
```

Externally sort by `content_id`, select canonicals, and write a cluster map. Join the narrow result back to text only after selection.

### MinHash signature storage

For 50 million records and 128 32-bit permutations, raw signatures require approximately:

$$50{,}000{,}000 \times 128 \times 4 = 25.6\text{ GB}$$

The signature matrix fits on disk and can be memory-mapped, but Python object overhead does not. With 256 permutations, raw signatures alone require approximately 51.2 GB. Start with 128 and validate recall.

Store:

```text
signatures.bin             # contiguous uint32 matrix
signature_doc_ids.parquet  # row index to doc_id
bands/                     # band-hash partitions
```

### LSH banding

Freeze the bands/rows configuration from labelled-pair experiments. For each band:

1. hash the band's signature slice;
2. emit `(band_hash, row_id)` to a partition;
3. externally sort/group by `band_hash`;
4. create candidate pairs within bounded groups;
5. skip or separately process pathological high-frequency buckets.

Deduplicate candidate pairs by external sort. Partition pair files by a stable prefix so no worker needs the full set.

### Candidate verification

Read only the two documents or compact shingle representations needed for each pair partition. Verify similarity before adding an edge. Batch reads by shard to avoid random I/O.

### Clustering

If the canonical ID can be resolved incrementally, use a partitioned union-find with periodic compaction. Otherwise create sorted edge files and process connected components in bounded batches. Checkpoint cluster state frequently.

### Canonical text selection

Build a narrow scoring table and use deterministic ordering. Join canonical IDs back to the full document table in a streaming pass.

### Failure controls

- cap maximum candidate pairs per bucket;
- cap total candidates per document and report capped records;
- checkpoint each band partition;
- retain signature files until cluster validation passes;
- audit high-degree clusters for boilerplate collapse.

---

## 8.5 Stage 3: quality scoring

Compute cheap deterministic features on CPU first. If a learned model is used:

1. save features and text references;
2. run one GPU inference process;
3. sort examples into dynamic token-length buckets;
4. begin with a conservative batch-token limit;
5. increase until throughput plateaus or memory reaches the normal ceiling;
6. write scores immediately rather than retaining them in RAM.

Use quantized inference only after score equivalence is validated. Record exact model revision and inference precision.

---

## 8.6 Stage 5: chunking

Chunk one Parquet batch at a time and preserve parent offsets. Expect the record count to grow sharply; budget by output bytes and chunk count, not document count.

Avoid materializing token lists for an entire shard. Tokenize one bounded batch, emit chunks, and release it. Keep the provisional tokenizer frozen for the full run.

---

## 8.7 Stage 6: optional KenLM scoring

Use memory-mapped KenLM binaries and sequential text scans. Store raw scores, normalized scores, and register calibration fields. Do not delete records in this pass unless a previously validated gate is enabled.

---

## 8.8 Stage 7: chunk dedup and boilerplate

Run exact chunk hashes first. This often captures a large share of redundancy cheaply.

For boilerplate:

1. fingerprint normalized lines;
2. count fingerprints by host/source partition;
3. identify repeated templates;
4. remove only high-confidence template spans;
5. preserve surrounding content and offsets.

Run fuzzy chunk dedup only on partitions where an audit shows meaningful residual duplication. Reuse Stage 2's disk-backed implementation; do not instantiate a second in-memory LSH index.

---

## 8.9 Stage 8: PII and safety inference

Perform deterministic detectors before model inference. Send only unresolved cases to GPU models. This reduces compute and permits different policy actions by category.

Store restricted match spans separately from distributable text. Never log raw sensitive values in general application logs.

---

## 8.10 Stage 9: decontamination

Build compact normalized benchmark indexes that remain memory-resident if possible. Stream corpus chunks through exact and n-gram checks. Send only approximate candidates to fuzzy verification.

Keep benchmark content and detailed match artifacts access-controlled. The public release should expose aggregate contamination statistics, not protected benchmark answers.

---

## 8.11 Stage 10: tokenizer experiments

Do not train every tokenizer on the complete corpus. Create one deterministic, stratified sample containing sufficient tokens from each source, domain, dialect route, and quality band.

Train candidates serially and clean superseded temporary files after metrics are captured. For downstream comparison, train small models with matched architecture, token budget, optimizer, seed policy, and evaluation suite.

---

## 9. Run orchestration

A stage runner should expose commands equivalent to:

```text
pipeline inspect --config run.yaml
pipeline benchmark --config run.yaml
pipeline run stage-00 --config run.yaml
pipeline validate stage-00 --run-id RUN_ID
pipeline resume stage-02 --run-id RUN_ID
pipeline report --run-id RUN_ID
pipeline cleanup --run-id RUN_ID --through-stage stage-01
```

The implementation may use a lightweight Python CLI rather than a distributed orchestration platform. On one machine, determinism and checkpointing matter more than infrastructure complexity.

### Task state

```text
PENDING -> RUNNING -> VALIDATING -> COMPLETE
                   -> FAILED -> RETRYABLE
                   -> QUARANTINED
```

Use a local metadata database with one row per task/shard. A stale `RUNNING` task is recoverable after checking its partial output.

---

## 10. Monitoring and stop conditions

Collect telemetry every 10–30 seconds:

- total and available unified memory;
- process RSS;
- GPU utilization and memory attribution;
- CPU utilization and load average;
- NVMe bytes read/written, latency, and free space;
- records and tokens processed;
- queue depth;
- stage error rate;
- candidate-pair growth.

### Automatic stop conditions

Checkpoint and stop when:

- free disk falls below 600 GB;
- used memory exceeds 108 GB for a sustained interval;
- filesystem or Parquet validation errors occur;
- output/input expansion exceeds the benchmark envelope;
- dedup candidate growth exceeds the configured safety multiple;
- reject or quarantine rate crosses a source-specific anomaly threshold;
- repeated worker failures exceed the retry budget.

---

## 11. Configuration template

```yaml
run:
  id: 2026-07-30_v3
  seed: 20260730
  code_revision: REQUIRED
  schema_version: 3

paths:
  root: /data/shuddhikaran
  raw: /data/shuddhikaran/raw
  run: /data/shuddhikaran/runs/2026-07-30_v3
  temp: /data/shuddhikaran/runs/2026-07-30_v3/tmp

resources:
  cpu_cores: 20
  normal_memory_limit_gb: 95
  emergency_memory_limit_gb: 108
  min_free_disk_gb: 600
  default_workers: 8
  max_workers: 12

parquet:
  compression: zstd
  compression_level: 6
  target_file_size_mb: 512
  row_group_size_mb: 128

language:
  model_revision: REQUIRED
  batch_records: 2048
  uncertain_to_quarantine: true

minhash:
  permutations: 128
  shingle_type: word
  shingle_width: 5
  seed: 20260730
  bands: CALIBRATE
  rows_per_band: CALIBRATE
  max_bucket_size: CALIBRATE
  max_candidates_per_doc: CALIBRATE

chunking:
  provisional_tokenizer_revision: REQUIRED
  max_tokens: 512
  preserve_paragraphs: true
  preserve_sentences: true

quality:
  learned_gate_enabled: false
  kenlm_gate_enabled: false

experimental:
  domain_classifier: false
  dialect_classifier: false
  fuzzy_chunk_dedup: false
  translation_rescue: false
  curriculum: false
```

Any `REQUIRED` or `CALIBRATE` value must block the full run.

---

## 12. Benchmark-based capacity estimation

For each stage calculate:

$$T_{full} = \frac{N_{full}}{R_{benchmark}} \times C_{contention} \times C_{safety}$$

where:

- $$N_{full}$$ is full records, tokens, or bytes;
- $$R_{benchmark}$$ is measured throughput;
- $$C_{contention}$$ accounts for mixed I/O and resource contention;
- $$C_{safety}$$ is normally 1.3–1.5.

Estimate storage as:

$$D_{peak} = D_{raw} + D_{input} + D_{output} + D_{temporary} + D_{reserve}$$

Do not proceed if the 90th-percentile estimate exceeds approximately 3.4 TB. Move raw snapshots or validated stage inputs to external storage before continuing.

---

## 13. Testing plan

### Unit tests

- source parser correctness;
- deterministic IDs;
- Unicode normalization idempotence;
- chunk offsets and reconstruction;
- canonical tie-breaking;
- configuration validation.

### Property tests

- every input record maps to accepted, quarantine, or rejected;
- rerunning a completed shard produces identical hashes;
- no duplicate canonical IDs within a cluster;
- chunk offsets remain within parent text;
- manifests reconcile with Parquet metadata.

### Integration tests

Run all stages on a small multi-source fixture containing exact duplicates, fuzzy duplicates, Bijoy text, code-mixing, malformed records, PII, benchmark contamination, and protected canonical text.

### Scale tests

Use the representative 1–5 million-document benchmark to trigger external sorting, spill paths, candidate caps, resume behavior, and low-disk controls.

---

## 14. Recommended execution sequence

### Pass 1: engineering validation

1. Inventory sources and licenses.
2. Build readers and golden fixtures.
3. Execute the representative benchmark.
4. Calibrate memory, workers, sharding, and disk multipliers.
5. Validate exact and fuzzy dedup on labelled pairs.

### Pass 2: production baseline

1. Stage 0 ingestion.
2. Stage 1 normalization and routing.
3. Stage 2 document deduplication.
4. Stage 3 deterministic quality scoring.
5. Stage 5 chunking.
6. Stage 7 exact chunk and boilerplate deduplication.
7. Stage 8 PII/safety policy processing.
8. Stage 9 decontamination.
9. Stage 10 tokenizer search.
10. Baseline mixing and pilot-model training.

### Pass 3: ablations

Evaluate one optional module at a time against the frozen baseline. Do not enable multiple experimental filters simultaneously because their effects become impossible to attribute.

### Pass 4: final release

Freeze configuration, regenerate manifests and checksums, run human audits, create corpus/tokenizer cards, and publish only artifacts permitted by source licenses.

---

## 15. Operational checklist

### Before a stage

- previous stage has `_SUCCESS`;
- input checksums pass;
- at least 600 GB free, preferably 800 GB or more;
- config has no unresolved placeholders;
- model and tokenizer revisions are pinned;
- output directory does not contain an unrelated run;
- benchmark envelope is available;
- rollback/reproduction path is confirmed.

### During a stage

- telemetry is active;
- queues remain bounded;
- memory stays below the normal ceiling;
- free disk stays above the stop threshold;
- throughput remains within the benchmark range;
- rejection and candidate rates do not show anomalies;
- partial shards and checkpoints advance.

### After a stage

- every shard validates;
- counts reconcile;
- checksums are written;
- audit samples pass;
- metrics are compared with the previous run;
- `_SUCCESS` is written;
- manifests/configs are backed up;
- cleanup is explicitly approved.

---

## 16. Final recommendations

1. Treat the DGX Spark as a capable single-node streaming system, not an in-memory cluster.
2. Make document and chunk dedup disk-backed from the first implementation.
3. Begin MinHash at 128 permutations and let labelled recall determine whether 256 is justified.
4. Run CPU-heavy and GPU-memory-heavy stages separately.
5. Keep 600–800 GB free throughout execution.
6. Use Parquet/Zstandard and narrow side tables to minimize repeated text I/O.
7. Benchmark real sources before estimating duration.
8. Keep the production baseline conservative; promote optional filters only through ablations.
9. Let pilot-model performance determine the final tokenizer, filters, and mixing strategy.
10. Preserve manifests and provenance even when intermediate text is removed from the local NVMe.
