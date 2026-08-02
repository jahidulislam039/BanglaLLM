# SHUDDHIKARAN: Final Bengali Pretraining Data Pipeline

**Version:** 3.0  
**Status:** Implementation-ready baseline  
**Primary target:** High-quality Bengali language-model pretraining corpus  
**Execution target:** NVIDIA DGX Spark, 128 GB unified memory, 4 TB NVMe

---

## 1. Objectives

SHUDDHIKARAN produces a reproducible, legally auditable, provenance-preserving Bengali pretraining corpus. The pipeline follows five principles:

1. **Preserve first, remove only with evidence.** Uncertain records go to quarantine rather than being silently discarded.
2. **Deduplicate globally and deterministically.** Exact and fuzzy duplicates are resolved across all sources while retaining cluster provenance.
3. **Separate production requirements from research experiments.** The baseline must not depend on speculative classifiers or synthetic translation.
4. **Measure quality by downstream utility.** Retention rate is diagnostic; pilot-model performance is the final arbiter.
5. **Make every stage restartable.** Inputs are immutable, outputs are sharded and atomic, and every run has a manifest.

---

## 2. Non-goals

The baseline pipeline does not require:

- automatic translation of rejected documents;
- a high-resolution dialect classifier;
- a large supervised domain classifier;
- strict easy-to-hard curriculum ordering;
- universal KenLM-based rejection;
- removal of all personal names or all safety-related discussion.

These are optional experiments and must pass explicit ablations before production use.

---

## 3. Data states

Every stage emits records into one of three states:

| State | Meaning | Next action |
|---|---|---|
| `accepted` | Meets the stage's validated criteria | Continue |
| `quarantine` | Ambiguous, malformed, or low-confidence | Review or process separately |
| `rejected` | Fails a documented, high-confidence rule | Retain metadata and reason |

No stage permanently deletes its input. Cleanup occurs only after output validation, checksum verification, manifest completion, and backup confirmation.

---

## 4. Canonical record schema

Use versioned Apache Arrow/Parquet schemas. Large text and repeated metadata must not be stored in Python object graphs.

```text
doc_id: string                  # Stable source-record identity
content_id: string              # Hash of normalized content
text: large_string
source_id: string
source_version: string
source_record_id: string?
source_url: string?
acquired_at: timestamp
published_at: timestamp?
license: string
redistribution_allowed: bool?
parent_doc_id: string?
chunk_index: int32?
start_offset: int64?
end_offset: int64?
language_scores: map<string,float>
script_stats: struct
encoding_operations: list<string>
quality_scores: map<string,float>
domain_scores: map<string,float>?
dialect_scores: map<string,float>?
pii_flags: list<string>
safety_flags: list<string>
dedup_cluster_id: string?
canonical_doc_id: string?
processing_version: string
text_hash: fixed_size_binary[32]
```

### Identity rules

- `doc_id` identifies a source record and remains stable across reruns.
- `content_id` identifies normalized content and changes if normalization changes.
- Hashes use BLAKE3 or SHA-256 consistently across the project.
- Normalization version is included in all content-derived identifiers.

---

## 5. Global run contract

Every stage directory contains:

```text
stage_NN/run_id/
├── config.yaml
├── manifest.json
├── metrics.parquet
├── accepted/
├── quarantine/
├── rejected/
├── samples/
└── _SUCCESS
```

The manifest records:

- code commit and dirty-state indicator;
- configuration hash;
- input and output manifests;
- model/tokenizer names, revisions, and checksums;
- random seeds;
- counts, bytes, and tokens in/out;
- reject and quarantine reasons;
- elapsed time and peak memory;
- shard checksums;
- schema version.

`_SUCCESS` is written last and only after all invariants pass.

---

# 6. Pipeline stages

## Stage −1 — Governance, source inventory, and evaluation freeze

### Purpose

Establish legal, technical, and evaluation constraints before processing begins.

### Required source registry

```yaml
source_id:
  version:
  acquisition_url:
  acquired_at:
  file_checksums:
  format:
  compression:
  license:
  redistribution_allowed:
  expected_language:
  expected_documents:
  parser_version:
  priority:
  personal_data_risk:
```

### Actions

1. Snapshot every source version and compute file-level SHA-256 checksums.
2. Record licensing, attribution, redistribution, and derivative-work constraints.
3. Freeze all known evaluation datasets and benchmark families.
4. Build a contamination registry containing prompts, contexts, answers, translations, and normalized variants.
5. Create stratified golden samples from every source and file format.
6. Define annotation guidelines for language, quality, deduplication, PII, and safety.

### Exit gate

Every source is checksummed, parsable, versioned, and assigned a documented usage policy. Unknown-license data cannot enter the distributable corpus.

---

## Stage 0 — Ingestion and canonicalization

### Purpose

Convert heterogeneous source files into the canonical document schema without destructive quality filtering.

### Actions

1. Stream source files; never fully decompress a corpus into memory.
2. Parse records with source-specific readers behind one common interface.
3. Preserve source identifiers, URLs, timestamps, titles, and license metadata.
4. Generate deterministic `doc_id` values.
5. Normalize line endings and reject only structurally unreadable records.
6. Write partitioned Parquet with Zstandard compression.

### Partitioning

Partition primarily by `source_id` and shard number. Avoid high-cardinality fields such as URL or document ID.

### Exit gate

- 100% of input files represented in the manifest;
- record counts reconcile with parser-level counters;
- random round-trip samples reproduce text and metadata;
- malformed records have explicit reasons.

---

## Stage 1 — Encoding repair, Unicode normalization, and language routing

### Purpose

Repair recoverable Bengali text and route documents by language confidence without over-filtering code-mixed or noisy material.

### Encoding process

1. Trust verified source-level encoding declarations first.
2. Test candidate decoders only when encoding is uncertain.
3. Compare Bengali character validity, replacement-character count, dictionary coverage, and lightweight language-model scores before and after conversion.
4. Apply Bijoy-to-Unicode conversion only when confidence exceeds a calibrated threshold.
5. Record every transformation and quarantine ambiguous cases.
6. Preserve raw-text linkage or a reversible transformation log.

### Normalization

Use a versioned Bengali-aware normalization policy. Apply Unicode normalization conservatively, normalize whitespace, and remove control characters only from an explicit denylist. Do not blanket-remove variation selectors or meaningful zero-width characters without a verified rule.

### Language routing

Combine:

- fastText or equivalent language probabilities;
- Bengali Unicode-script ratio;
- alphabetic-token ratio;
- document length;
- line-level language distribution;
- source metadata;
- romanized-Bengali indicators.

Suggested routes:

- `bn_native_high_confidence`
- `bn_code_mixed`
- `bn_romanized_candidate`
- `multilingual_candidate`
- `non_bn_high_confidence`
- `uncertain_language`

Thresholds must be calibrated against a manually labelled, source-stratified sample. A universal 70% Bengali threshold is not a production rule.

### Exit gate

Report precision and recall by source and route. High-confidence Bengali recall is the primary gate; retention percentage alone is not.

---

## Stage 2 — Global document deduplication

### Purpose

Remove exact and near-duplicate documents across all sources while preserving provenance and minority variants.

### Step 2A: exact deduplication

1. Compute normalized `content_id`.
2. Externally sort/group by hash.
3. Create a duplicate cluster for each group.
4. Choose one canonical record deterministically.

### Step 2B: fuzzy candidate generation

1. Create word or character shingles using a frozen configuration.
2. Compute fixed-seed MinHash signatures shard-by-shard.
3. Store signatures in binary arrays or Parquet.
4. Emit LSH band hashes.
5. Partition by band hash and externally sort/group.
6. Generate bounded candidate pairs; cap pathological buckets.

Start with 128 permutations and increase only if labelled-pair evaluation demonstrates a material recall benefit.

### Step 2C: candidate verification

Estimate or compute actual shingle similarity for candidate pairs. Do not treat an LSH collision as proof of duplication.

### Step 2D: clustering and canonical selection

Build connected components using disk-backed pair files plus union-find or a scalable graph pass. Canonical selection considers, in order:

1. redistribution rights;
2. extraction quality;
3. completeness;
4. authoritative source status;
5. metadata quality;
6. publication timestamp;
7. deterministic source priority and `doc_id` tie-break.

Retain all contributing source IDs and record IDs in cluster metadata.

### Protected categories

Apply conservative rules to dialects, poems, laws and amendments, religious or literary canonical passages, translations, and parallel corpora. Similarity is not always redundancy.

### Exit gate

Evaluate exactness, pair precision, pair recall, cluster quality, and source/dialect loss on manually labelled pairs. Record deduplicated token loss as well as document loss.

---

## Stage 3 — Baseline quality scoring and structural cleanup

### Purpose

Remove obvious extraction failures and rank quality without letting source identity become a proxy for quality.

### Deterministic features

- Unicode and replacement-character validity;
- alphabetic and punctuation ratios;
- repeated line and repeated n-gram ratios;
- sentence and paragraph structure;
- boilerplate indicators;
- token-length extremes;
- HTML/navigation residue;
- entropy and character-distribution anomalies.

### Learned quality model

If used, train on human-labelled examples sampled across every source. Include positive and negative examples from the same source. Split evaluation by hostname/domain/source to prevent leakage.

Store calibrated quality scores and use three regions:

- accept;
- uncertain/quarantine;
- reject.

### Exit gate

Report PR-AUC, calibration error, subgroup false-positive rates, and source-held-out performance. Wikipedia-versus-WET discrimination is not an acceptable validation strategy.

---

## Stage 4 — Optional metadata enrichment

### Purpose

Attach domain, register, and dialect metadata for analysis and sampling—not mandatory rejection.

### Baseline

Use source metadata and transparent weak rules, retaining `unknown` when confidence is low.

### Optional classifiers

Introduce learned classifiers only after creating manually labelled, source-balanced evaluation sets. Store probability distributions rather than hard labels.

Never map uncertain dialect records to `shuddho` by default.

### Exit gate

Enrichment may enter production only if it improves corpus diagnostics or downstream sampling without unacceptable subgroup error.

---

## Stage 5 — Boundary-preserving document segmentation

### Purpose

Create trainable segments while preserving discourse structure and traceability.

### Rules

1. Use a frozen provisional tokenizer or Unicode-word count; record its version.
2. Split at paragraph boundaries first, then sentence boundaries.
3. Apply hard token splits only as a last resort.
4. Store source offsets, `parent_doc_id`, and `chunk_index`.
5. Do not insert synthetic markers such as `[...]` into training text.
6. Route short segments by content type instead of discarding all of them.

Recommended initial maximum: 512 provisional tokens, subject to sequence-length and tokenizer experiments.

### Exit gate

Measure sentence fragmentation, length distribution, reconstruction coverage, and short-content retention by domain.

---

## Stage 6 — Perplexity and quality ranking

### Purpose

Rank suspicious text using language-model signals without treating uncommon language as automatically low quality.

### Rules

- KenLM or another lightweight LM is one feature, not an automatic global decision.
- Calibrate scores by broad register or domain when sufficient data exists.
- Preserve exceptions for high-value authoritative sources.
- Route score extremes to review before enabling rejection.
- Evaluate dialect, technical, historical, OCR, and code-mixed false positives.

### Exit gate

A perplexity gate becomes destructive only after labelled validation and pilot-model ablation show improvement.

---

## Stage 7 — Chunk-level exact deduplication and boilerplate removal

### Purpose

Remove repeated training segments introduced by chunking and web templates.

### Ordered process

1. Exact normalized chunk deduplication.
2. Repeated-line fingerprinting.
3. Host/template-level boilerplate detection.
4. Optional fuzzy chunk dedup for eligible domains.
5. Protected handling for laws, quotations, poetry, religious text, and parallel data.

A second global Python in-memory MinHash index is prohibited. Fuzzy chunk dedup uses the same disk-backed architecture as Stage 2 and must justify its incremental benefit.

### Exit gate

Report incremental unique-token gain, protected-category loss, and fuzzy-pair precision. Disable fuzzy chunk dedup if it provides little downstream benefit.

---

## Stage 8 — PII and safety policy processing

### Purpose

Reduce privacy and policy risk without erasing legitimate journalism, history, biography, law, or counterspeech.

### PII categories

Distinguish:

- sensitive identifiers;
- private contact details;
- private individuals;
- public figures;
- organizations and institutional addresses;
- authors and bylines.

Do not redact all person names. Apply detection and transformation policies per category, recording redaction type and confidence.

### Safety categories

Distinguish illegal or exploitative material, explicit sexual content, hateful advocacy, quoted reporting, historical discussion, educational content, and counterspeech. Use quarantine for context-dependent cases.

### Exit gate

Human review measures precision and false-positive rates for every destructive policy, stratified by source and content type.

---

## Stage 9 — Evaluation decontamination

### Purpose

Prevent benchmark leakage before tokenizer finalization or model training.

### Matching layers

1. Normalized exact document and segment hashes.
2. Prompt, context, and answer matching separately.
3. Token n-gram overlap.
4. MinHash or document-similarity matching.
5. Translated and normalized benchmark variants where practical.
6. Benchmark-family and source-level holdouts.

All removed records retain benchmark family, match method, score, and provenance in a restricted audit table.

### Exit gate

Produce a contamination report per benchmark with exact and approximate removal counts and reviewed false-positive samples.

---

## Stage 10 — Tokenizer experiments

### Purpose

Select a tokenizer empirically rather than fixing algorithm and vocabulary size in advance.

### Candidate grid

- Unigram: 32K, 48K, 64K
- BPE: 32K, 48K, 64K
- byte fallback: enabled/disabled
- normalization variants
- explicit grapheme-safe preprocessing variants

### Evaluation metrics

- token fertility by source/domain/dialect;
- bytes or characters per token;
- unknown and byte-fallback rates;
- sequence-length distribution;
- Bengali grapheme fragmentation;
- morphology-boundary alignment on a labelled sample;
- compression ratio;
- small-model validation loss.

Standard SentencePiece does not itself guarantee morphology-prioritized merges or grapheme safety; those claims require explicit preprocessing or algorithmic changes and validation.

### Exit gate

Choose the tokenizer on a multi-metric scorecard plus pilot-model results.

---

## Stage 11 — Corpus mixing and optional curriculum experiments

### Baseline

Use globally shuffled, temperature-based sampling over source, quality, domain, and language route. Cap dominant sources and ensure minimum exposure for valuable minority partitions.

### Experiments

Compare:

- global shuffle;
- quality-weighted sampling;
- source/domain temperature sampling;
- competence-based curriculum;
- strict quality ordering.

Low quality is not synonymous with high difficulty. Strict curriculum ordering enters production only when controlled pilots improve held-out loss or downstream evaluation.

---

## Stage 12 — Final corpus assembly and release

### Outputs

- tokenized and untokenized Parquet shards;
- deterministic shuffle mapping;
- source and license manifests;
- dedup cluster map;
- transformation and rejection reports;
- contamination report;
- tokenizer model and evaluation report;
- corpus card and known-limitations report;
- restricted PII/safety audit artifacts;
- reproducibility bundle containing code/config/model revisions.

### Final acceptance gates

1. All stages have `_SUCCESS` and checksum-valid manifests.
2. No unknown-license records enter distributable outputs.
3. Human audit meets agreed quality and privacy thresholds.
4. Source, domain, dialect, and code-mixed representation are documented.
5. Pilot-model evaluation improves over the previous corpus baseline.
6. Total token budget and unique-token ratio are measured after final deduplication.

---

# 7. Required metrics at every stage

Report both document-weighted and token-weighted values:

- records, tokens, and bytes in/out;
- accepted, quarantined, and rejected counts;
- counts by source and language route;
- removal reasons;
- throughput and wall time;
- peak process and system memory;
- temporary and final disk usage;
- random audit samples;
- regression against the previous accepted run.

Retention targets are alerts, not goals. Unexpectedly high retention can indicate ineffective filtering; unexpectedly low retention can indicate data loss.

---

# 8. Validation strategy

## Unit tests

Test every parser, normalizer, hash rule, routing feature, and schema conversion.

## Property tests

Verify idempotent normalization, stable IDs, deterministic sharding, valid offsets, and no record loss across accepted/quarantine/rejected outputs.

## Golden tests

Maintain source-stratified examples for encoding repair, language routing, duplicate pairs, quality, PII, safety, and decontamination.

## Shadow runs

Run new configurations against a fixed representative subset and compare all metrics before full-corpus execution.

## Pilot-model ablations

For consequential filters, train small matched models on baseline and experimental corpus variants. Keep only changes that improve predefined downstream metrics without unacceptable subgroup regression.

---

# 9. Production baseline versus experiments

## Production baseline

- governed ingestion;
- conservative normalization and routing;
- exact and disk-backed fuzzy document deduplication;
- deterministic quality features;
- boundary-preserving segmentation;
- exact chunk and boilerplate deduplication;
- contextual PII and safety handling;
- multi-layer decontamination;
- tokenizer search;
- shuffled temperature-based mixing.

## Experimental modules

- learned quality ranker;
- learned domain/dialect classifiers;
- KenLM rejection;
- fuzzy chunk deduplication;
- romanized-Bengali transliteration;
- synthetic translation rescue;
- curriculum ordering.

Each experiment is disabled by default, versioned independently, and promoted only after passing data-quality and pilot-model gates.
