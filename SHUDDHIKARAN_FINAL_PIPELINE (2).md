# SHUDDHIKARAN: A Retention-First Pre-Training Data Pipeline for Bengali Language Models

**Version 3.0 — Final (v2 architecture, hardened for reproducible execution)**

---

## Abstract

This document specifies **SHUDDHIKARAN** (শুদ্ধিকরণ — *purification*), a pre-training data pipeline engineered for the construction of a Bengali-native large language model (LLM). The pipeline processes raw Bengali text from a large set of heterogeneous sources and is designed around a central thesis: **current data processing methodologies for low-resource languages are excessively wasteful.** Filtering strategies developed for English-scale data abundance (15T+ tokens) are routinely applied to Bengali corpora, where the marginal cost of discarding a valid document is permanent and irrecoverable information loss.

SHUDDHIKARAN inverts this default. The pipeline is architected to *rescue first and drop last*, deploying multi-path routing, second-chance validation, and domain-conditional quality gates to maximize the volume of clean, high-quality Bengali text surviving into the final training corpus.

**Version 2.0** established the architecture used here: a curated WARC-extracted backbone (FineWeb2, HPLT v2, Sangraha), global early deduplication, a model-based quality classifier used as a ranker, model sizing decoupled from the Chinchilla cap, and explicit infrastructure guidance.

**Version 3.0 keeps that architecture intact and makes it executable.** It adds no new stages to the conceptual pipeline and removes none. It changes *how* the stages are run:

1. **Stage −1 (Governance & Source Registry) is added ahead of Stage 0.** No source enters the pipeline without a license, checksum, version, and parser record. This is the difference between a corpus that can be released and one that cannot.
2. **Deduplication is redesigned as an external-memory (disk-backed) computation.** The v2 design assumed an in-RAM `datasketch` LSH index. At 50M+ documents that does not fit in 128 GB alongside everything else, so Stage 2 and Stage 7 become sort-and-group passes over Parquet.
3. **Every hard threshold becomes a calibrated, quarantine-backed decision.** Borderline documents go to a `quarantine/` path rather than being silently dropped or silently passed. Nothing is dropped on an uncalibrated constant.
4. **Every stage becomes deterministic, resumable, and auditable.** Atomic output publication, `_SUCCESS` manifests, config/code hashes, per-shard retries, and token-weighted metrics alongside document-weighted ones.
5. **Speculative components move behind ablation gates, and estimates become benchmark-derived.** Domain/dialect classifiers, the KenLM fleet, LLM rescue, and curriculum ordering are all optional. No runtime, memory, storage, or retention number is published until measured on a 1% benchmark slice.
6. **Hardware assumptions are corrected.** DGX Spark is a **20-core Arm CPU** with 128 GB unified memory and 4 TB NVMe — not a 72-core Grace system. All concurrency and memory planning follows from that.

The pipeline retains all original methodological contributions — chunk-level rescue, domain-conditional perplexity gating with dialect-aware fallback, dual-path code-mixed processing, morphology-aware tokenizer co-design, curriculum learning, and a built-in ablation framework.

---

## Table of Contents

1. [Project Objective & Design Philosophy](#1-project-objective--design-philosophy)
2. [Raw Data Inventory](#2-raw-data-inventory)
3. [Pipeline Architecture](#3-pipeline-architecture)
4. [Stage −1 — Governance, Source Registry & Benchmarking](#4-stage-1--governance-source-registry--benchmarking)
5. [Stage 0 — Encoding Foundation](#5-stage-0--encoding-foundation)
6. [Stage 1 — Document-Level Pre-screening](#6-stage-1--document-level-pre-screening)
7. [Stage 2 — Global Deduplication (Early, External-Memory)](#7-stage-2--global-deduplication-early-external-memory)
8. [Stage 3 — Model-Based Quality Scoring](#8-stage-3--model-based-quality-scoring)
9. [Stage 4 — Domain & Dialect Classification](#9-stage-4--domain--dialect-classification)
10. [Stage 5 — Sub-Document Processing](#10-stage-5--sub-document-processing)
11. [Stage 6 — Dual-Path Code-Mixed Processing](#11-stage-6--dual-path-code-mixed-processing)
12. [Stage 7 — Chunk-Level Deduplication](#12-stage-7--chunk-level-deduplication)
13. [Stage 8 — Final Quality Control](#13-stage-8--final-quality-control)
14. [Stage 9 — Evaluation Set Decontamination](#14-stage-9--evaluation-set-decontamination)
15. [Stage 10 — Tokenizer Co-Design & Fertility Validation](#15-stage-10--tokenizer-co-design--fertility-validation)
16. [Stage 11 — Curriculum Scoring & Training Manifest](#16-stage-11--curriculum-scoring--training-manifest)
17. [Stage 12 — Corpus Assembly, Model Sizing & Audit](#17-stage-12--corpus-assembly-model-sizing--audit)
18. [Engineering & Infrastructure](#18-engineering--infrastructure)
19. [Reproducibility, Data Contracts & Stage Gates](#19-reproducibility-data-contracts--stage-gates)
20. [Comparison with Published Approaches](#20-comparison-with-published-approaches)
21. [Novel Contributions](#21-novel-contributions)
22. [Ablation Framework](#22-ablation-framework)
23. [Execution Roadmap & Priority Order](#23-execution-roadmap--priority-order)
24. [References](#24-references)

---

## 1. Project Objective & Design Philosophy

### 1.1 Objective

Ground-up pre-training of a generative Bengali foundation model, sized to be **as large as the available data and compute reasonably allow**. The model is trained exclusively on data processed through this pipeline.

### 1.2 Model Sizing — Beyond the Chinchilla Cap

Version 1.0 fixed the model size at the Chinchilla-optimal ratio:

```
model_parameters = surviving_clean_tokens / 20
```

**This is not a hard cap.** Chinchilla's 20:1 ratio (Hoffmann et al., 2022) is *compute-optimal for a fixed one-time training budget* — it minimizes loss per FLOP. It is **not** how modern foundational models are built. Llama 2/3, Qwen, and Gemma are all deliberately *over-trained*, using 100–200+ tokens per parameter, because training compute is paid once but inference savings last forever.

Two consequences:

1. **The real data ceiling is not "unique tokens ÷ 20."** Under data-constrained scaling (Muennighoff et al., 2023), pretraining data can be repeated for up to roughly **4 epochs with almost no loss penalty**, so the effective budget is approximately `unique_tokens × 4`.

2. **The most important lever is more *unique* data**, not the sizing formula. This is why the Tier 1 backbone (Section 2) and global dedup (Section 7) exist — together they determine the true unique-token count that everything else follows from.

**v3.0 qualification:** the epoch multiplier is a *planning assumption, not a guarantee*. Repetition tolerance depends on corpus quality, duplication residue, and model size. Validate it in the Stage 12 pilot (Section 17.6) before committing to a 4-epoch full run. If the pilot shows degradation at epoch 2–3, the multiplier drops and the recommended model size drops with it.

The final model size is chosen at Stage 12 as a **joint function of unique tokens, the *validated* epoch count, and the training compute actually secured**.

### 1.3 Design Philosophy

**Principle 1 — Retention over aggression.** For a language with finite available data, the cost of rescuing a marginal document is a few CPU cycles; the cost of dropping a valid document is permanent information loss. Every filtering threshold must justify the data it discards, and every drop decision must be compensated by a rescue path where feasible. Wherever possible the pipeline *scores and ranks* rather than hard-drops.

**Principle 2 — Quality at the right granularity.** Document-level filtering is appropriate for English, where 15 trillion tokens of replacement data exist. For Bengali, quality control operates at *chunk granularity* — a document containing 40% noise and 60% valid Bengali still contributes 60% of irreplaceable text when processed at the sub-document level.

**Principle 3 — Data engineering, not just data cleaning.** The pipeline's outputs include quality scores, domain labels, dialect annotations, tokenizer fertility measurements, curriculum difficulty scores, and a training-order manifest. Data composition, ordering, and tokenizer co-design are first-class pipeline products.

**Principle 4 — Spend compute only on data that will survive.** Expensive operations (LLM translation, per-chunk perplexity, tokenization) run *after* deduplication and cheap filtering, never before.

**Principle 5 (new in v3.0) — Nothing is dropped on an unvalidated constant.** Every threshold in this document is a *hypothesis with a default value*. Before it is allowed to delete data at scale it must be calibrated on a labelled sample, and text near the boundary is routed to `quarantine/` rather than deleted. Quarantine is reviewable; deletion is not.

**Principle 6 (new in v3.0) — Reproducible or it did not happen.** Every stage records its config hash, code commit, model hashes, and random seeds, and publishes output atomically behind a `_SUCCESS` marker. A number that cannot be regenerated is not a result.

**Principle 7 (new in v3.0) — Provenance and licensing are part of the data.** A technically clean corpus that cannot be released is a failed deliverable. Licensing, redistribution rights, and per-document provenance are tracked from Stage −1 onward.

### 1.4 Conventions

| Marker | Meaning |
|--------|---------|
| `SOTA` | Matches current best practice from published work (FineWeb2, DCLM, REWIRE, etc.) |
| `BEYOND-SOTA` | Exceeds what any published Bengali or multilingual pipeline has demonstrated |
| `NOVEL` | Original contribution with no published precedent in Bengali NLP |
| `HYPOTHESIS` | **(new in v3.0)** A default value or design choice that must be calibrated or ablated before it is trusted to remove data |

Any threshold table in this document marked `HYPOTHESIS` is a starting point for calibration, not a validated constant.

---

## 2. Raw Data Inventory

The corpus is organized into two tiers: a **curated backbone** of pre-cleaned, WARC-extracted sources that form the quality core, and a **supplementary tier** of the project's own crawls and specialist collections that add volume, register coverage, and rare domains.

### 2.1 Tier 1 — Curated Backbone (WARC-extracted, pre-filtered)

| Source | Nature | Why it matters |
|--------|--------|----------------|
| **FineWeb2 (`bn_Beng`)** | WARC-extracted, deduplicated, quality-filtered web text | Highest-quality open web Bengali in existence. Primary backbone. |
| **HPLT v2 (`ben_Beng`, sorted)** | Cleaned, document-level, quality/register-sorted shards | Large clean web corpus with per-document quality metadata. See Section 2.4. |
| **AI4Bharat Sangraha (Bengali)** | Verified + synthetic, includes romanized Bengali | Adds the romanized/"Banglish" register that is otherwise almost absent. |
| **MADLAD-400 (`bn`)** | Cleaned multilingual CC derivative | Extra coverage and a cross-dedup reference. |

> **On WARC vs. WET.** The original pipeline extracted CommonCrawl text from **WET** files (CommonCrawl's naive text dump, which retains navigation, menus, and boilerplate). Re-extracting from **WARC** with Trafilatura/resiliparse produces far cleaner documents and is what FineWeb2 does. **This project does not re-extract from WARC**, deliberately: the Tier 1 backbone is *already* WARC-extracted and SOTA-filtered, so the high-quality core is covered without spending the time. The project's existing WET crawls are retained as a supplementary recall source (Tier 2), where WET-grade cleanliness plus the quality classifier (Stage 3) is adequate.

**v3.0 addition — Tier 1 sources must still be verified, not trusted.** Publisher filtering is not a substitute for our own validation. For every Tier 1 source, record the exact snapshot/revision ID, verify checksums, and confirm the license permits redistribution in a derived corpus. Sangraha's *synthetic* portion must be tagged distinctly at ingest, because it counts against the synthetic cap in Section 17.2 exactly like our own LLM rescue output does.

### 2.2 Tier 2 — Supplementary & Specialist Sources

| Source | Volume | Format | Domain | Role |
|--------|--------|--------|--------|------|
| Own WET CommonCrawl (10 crawls, processed) | ~variable | JSONL | Mixed web | Recall / volume; ranked low by quality classifier |
| mC4 (Bengali) | 43.89 GB | `.txt` parts | Mixed web | Recall (heavy overlap with backbone) |
| IndicCorpV2 (Bengali) | 14.89 GB | `.txt` monolith | Mixed web | Recall |
| CC-100 (Bengali) | 8.41 GB | `.txt` monolith | Mixed web | Recall |
| Bangla Newspaper Dataset (ebD) | 21.24 GB | `.json` | News | Long-form journalism |
| Potrika Newspaper Archive | 3.99 GB | Mixed | News | Journalism |
| bdnews24 | 1.54 GB | Mixed | News | Journalism |
| Prothom Alo (2016–2025) | 1.28 GB | Mixed | News | Premium journalism |
| Bengali Wikipedia (Arrow) | 1.18 GB | `.arrow` | Encyclopedic | High-value reference |
| Samanantar | 1.12 GB | Mixed | Parallel (Bengali side) | Clean sentences |
| 1000 Day News | 0.75 GB | Mixed | News | Journalism |
| OCR'd NCTB Textbooks | 0.08 GB | `.txt` | Educational | Long-form, high value |
| Banglapedia | 0.06 GB | Mixed | Encyclopedic | Curated reference |
| Legal Documents | 0.05 GB | `.txt` | Legal | Likely Bijoy-encoded |
| Old HSC Materials | 0.02 GB | Mixed | Educational | Rare register |
| Poem Dataset | 0.01 GB | Mixed | Literature | Rare register |
| Medical Dataset | <0.01 GB | Mixed | Medical | Rare domain |
| YouTube subtitles (optional) | TBD | text | Spoken/informal | Unique spoken register |

**v3.0 licensing warning.** Several Tier 2 sources carry redistribution risk that must be resolved at Stage −1, not at release time:

| Source class | Risk | Required action |
|--------------|------|-----------------|
| Newspaper archives (ebD, Prothom Alo, Potrika, bdnews24, 1000 Day News) | Publisher copyright; scraped archives rarely permit redistribution | Classify as `train-only` or `excluded`; do not assume release rights |
| OCR'd NCTB / HSC materials | Textbook copyright | Confirm government licensing terms |
| Poem dataset / literature | Author copyright, possibly in-copyright | Verify per-item; exclude if unresolved |
| Samanantar, Sangraha, FineWeb2, HPLT, MADLAD, mC4, CC-100, IndicCorpV2 | Permissive but attribution-bound | Record exact license string and version |

Each source is assigned a **release class**: `redistributable`, `train-only`, or `excluded`. The final corpus is materialized in two views — a full training view and a releasable public view — from the same provenance records.

### 2.3 The Overlap Problem — Why Global Dedup Is Central

The backbone and the supplementary tier are **heavily overlapping**, because most are derived from CommonCrawl:

| Source Pair | Expected Overlap | Reason |
|------------|-----------------|--------|
| FineWeb2 ↔ HPLT ↔ MADLAD ↔ mC4 ↔ CC-100 ↔ IndicCorpV2 ↔ own WET | High | All CommonCrawl-derived |
| Prothom Alo ↔ ebD | 50%+ | ebD likely contains Prothom Alo articles |
| News sources ↔ each other | 15–50% | Wire syndication (BSS, UNB) |

**Consequence:** raw gigabytes are *not* a meaningful measure of corpus size. The true unique-token count only emerges after the global deduplication pass (Stage 2). Adding a new CC-derived source typically yields far fewer net-new tokens than its raw size suggests; adding a *curated* source like FineWeb2 or a *specialist* source like OCR'd books yields proportionally more.

### 2.4 Note on the Downloaded HPLT Shards

The HPLT v2 shards obtained from `data.hplt-project.org/three/sorted/ben_Beng/` (files `5_1` through `10_1`) are the **quality/register-sorted** release. Two checks apply:

- **Confirm the metadata is preserved.** Each document should carry HPLT's per-document language and quality scores. Keep these — they let us rank HPLT text without re-running our own KenLM on it.
- **Lower buckets (1–4) were not downloaded.** These are the noisier tail. Because they cost only download bandwidth and the global dedup + quality classifier will sort them out anyway, it is recommended to also pull buckets 1–4 and let the pipeline decide, rather than dropping them unseen. This is consistent with the retention-first principle.

### 2.5 Token Estimates Are Benchmark-Derived, Not Assumed `HYPOTHESIS`

Version 1.0 published a bytes-per-token table (≈6 / ≈9 / ≈14) and a retention target of 65–75%. **v3.0 treats all such numbers as unknown until measured**, because bytes-per-token depends on the tokenizer that does not exist until Stage 10, and retention depends on thresholds that are not yet calibrated.

The Stage −1 benchmark (Section 4.4) produces the real values on a 1% stratified slice:

| Quantity to measure | Where it is used |
|---------------------|------------------|
| Compressed → uncompressed expansion ratio per source | Storage budget |
| Documents per GB per source | Shard sizing |
| Bytes per token under the provisional tokenizer | Token estimate |
| Duplicate rate per source pair | Expected Stage 2 retention |
| Per-stage throughput (docs/s, MB/s) | Schedule |

Publish token and retention figures as **ranges with a measurement date and config hash**, never as single confident numbers.

---

## 3. Pipeline Architecture

The stage order is inherited from v2: **cheap filtering and global deduplication happen before any expensive per-chunk or LLM work.** v3.0 adds a governance stage in front and a quarantine lane throughout.

```
                        Stage -1: Governance, Source Registry & Benchmarking      <-- NEW in v3.0
                        (licenses, checksums, versions, parsers, 1% benchmark,
                         frozen eval sets, golden samples)
                                     |
Tier 1 backbone (FineWeb2, HPLT, Sangraha, MADLAD)  +  Tier 2 supplementary (WET, mC4, news, books, ...)
    |
    v
Stage 0: Encoding Foundation
    |    Bijoy->Unicode (evidence-based), NFC, ZWNJ canonicalization, PUA stripping
    |    Retention: ~100% (repair, not removal)  |  uncertain conversions -> QUARANTINE
    v
Stage 1: Document-Level Pre-screening
    |    Composite routing score: LID + script ratio + validity + length + source prior
    |    Retention target: >=90%  |  borderline -> QUARANTINE
    v
Stage 2: GLOBAL Deduplication (early, EXTERNAL-MEMORY)
    |    Exact (SHA-256) + fuzzy (banded MinHash, sort-and-group on disk) ACROSS ALL SOURCES
    |    Emits per-source dedup stats -> true unique-token count
    |    Duplicate clusters retain ALL contributing source IDs
    |    Retention: ~60-80% of documents (to be measured)
    v
Stage 3: Model-Based Quality Scoring
    |    Learned quality classifier scores every document (rank, don't drop)
    |    Trained with source-leakage controls + human labels
    |    Retention: ~100%
    v
Stage 4: Domain & Dialect Classification  (TIERED: rules -> light model -> full classifier)
    |    10-domain taxonomy, 6-dialect detection, `unknown` allowed
    |    Retention: ~100% (labelling only)
    |
    +----------------------------------------------+
    v                                              v
Stage 5a: Clean Bengali Chunks                 Stage 5b: Code-Mixed Chunks
    |    Boundary-preserving chunking               |    Bengali script ratio 0.25-0.80
    |    KenLM as a FEATURE (+ optional gate)        v
    |                                        Stage 6: Dual-Path Code-Mixed Processing
    |                                          (native preservation default; LLM rescue ablation-gated)
    +-------------------------------+---------------+
                                     v
                        Stage 7: Chunk-Level Deduplication
                        (exact + boilerplate first; fuzzy only where validated)
                                     v
                        Stage 8: Final Quality Control
                        (contextual PII policy, safety with reporting/counterspeech carve-outs)
                                     v
                        Stage 9: Evaluation Set Decontamination
                        (multi-signal: exact + n-gram + MinHash + field-wise)
                                     v
                        Stage 10: Tokenizer Co-Design & Fertility Validation
                        (candidate sweep, empirical vocab size)
                                     v
                        Stage 11: Curriculum Scoring & Training Manifest
                        (OPTIONAL; shuffled baseline is the default)
                                     v
                        Stage 12: Corpus Assembly, Model Sizing & Audit
                        (pilot-validated epochs; dual release views)
                                     v
                        Model Architecture Specification + Training Plan
```

**Key ordering properties (unchanged from v2):**

- **Global deduplication runs early**, before quality scoring, chunking, KenLM, and any LLM translation. In v1, dedup happened after Qwen-72B had already translated code-mixed chunks that were about to be deleted. That is now impossible.
- **A model-based quality classifier sits right after dedup**, so every later stage can use the quality score.
- **Chunk-level dedup is separate** from global document dedup, since it targets a different granularity and runs after chunking.

**Key v3.0 additions:**

- A **governance stage** precedes everything.
- A **quarantine lane** runs alongside `accepted/` and `rejected/` at every stage.
- **Cumulative retention is no longer multiplied out as a headline claim.** The v1/v2 estimate (0.90 × 0.85 × 0.80 × 0.95 × 0.98 ≈ 60–70%) assumed independent per-stage rates and document-weighted counting. v3.0 reports measured per-stage retention in **both** document-weighted and token-weighted form, and computes the cumulative figure only from measurements.

---

## 4. Stage −1 — Governance, Source Registry & Benchmarking `NEW in v3.0`

**Purpose:** Establish the legal, provenance, and measurement foundations before a single document is processed. This stage produces no corpus text. It produces the records that make the corpus releasable and the estimates that make the schedule real.

**Why it is first:** the two most common ways a project like this fails are (a) discovering at release time that half the corpus cannot be redistributed, and (b) discovering at month two that the storage and runtime estimates were wrong by an order of magnitude. Both are prevented here.

### 4.1 Immutable Source Registry

Every source gets one YAML record, version-controlled, written once and amended only by appending:

```yaml
source_id: hplt_v2_ben_sorted
version: "three/sorted/ben_Beng, buckets 1-10"
acquisition_url: https://data.hplt-project.org/three/sorted/ben_Beng/
acquired_at: 2026-01-15T00:00:00Z
file_manifest: manifests/hplt_v2_ben_sorted.sha256   # per-file SHA-256
format: jsonl.zst
compression: zstd
license: cc0-1.0
release_class: redistributable        # redistributable | train-only | excluded
redistribution_allowed: true
attribution_required: true
personal_data_risk: medium
synthetic_content: false
expected_language: ben_Beng
expected_documents: null              # filled after benchmark
parser_version: readers/hplt@v1
tier: 1
source_priority: 2
notes: "Carries publisher quality + language scores; preserve them."
```

**Required Stage −1 outputs:**

- File-level **SHA-256 manifest** for every source.
- **License compatibility matrix** and the resulting `release_class` per source.
- **Excluded-source register** with the reason for exclusion.
- **Evaluation-set registry** (Section 4.3).
- **Golden-sample set** (Section 4.2).
- **Benchmark report** (Section 4.4).

### 4.2 Golden Samples

Draw a stratified sample of **500–1,000 documents per source** and freeze it. Every parser, every threshold, and every classifier is regression-tested against these samples for the life of the project. Deliberately include:

- shortest and longest documents
- Bijoy-suspected files
- OCR output
- romanized Bengali
- code-mixed text
- table-heavy and list-heavy documents
- documents with mixed scripts
- known cross-source duplicates

### 4.3 Freeze Evaluation Sets Now, Not at Stage 9

v2 collected the decontamination blocklist at Stage 9. That is too late: by then the eval splits may already have been used for classifier training or threshold calibration. **Register and freeze every evaluation set at Stage −1**, with a checksum, and treat them as untouchable inputs:

- TituLLMs BLUB benchmark (5 sets)
- BanglaBERT original test sets
- NCTB textbook test material
- BanglaMath evaluation set
- DIALTSA-BN dialectal evaluation sets
- BnSentMix code-mixed evaluation set
- Custom BanglaLM-Eval prompt-response pairs
- **Held-out internal validation splits**, drawn per-source and per-domain, reserved before any filtering

### 4.4 Mandatory Benchmark Slice

Run the full pipeline end-to-end on **1% of every source (minimum 1M, target 5M documents)** before scaling. Measure, per stage and per source:

| Metric | Purpose |
|--------|---------|
| documents/second, MB/second (compressed and uncompressed) | Schedule |
| peak RSS per worker | Concurrency limits |
| output/input byte ratio | Storage budget |
| MinHash signatures/second; candidate pairs per document | Dedup feasibility |
| GPU tokens/second (classifier, LLM rescue) | GPU stage cost |
| duplicate rate per source pair | Expected Stage 2 retention |
| chunk expansion factor (chunks per document) | Stage 7 volume |

**Exit gate for Stage −1:** every source is readable by a versioned parser, checksummed, license-classified, benchmarked, and represented in the golden samples; all evaluation sets are frozen. The pipeline does not proceed on any source that fails this gate.

---

## 5. Stage 0 — Encoding Foundation

**Purpose:** Repair encoding-level corruption before any downstream processing. Encoding errors propagate silently and corrupt all filtering, hashing, deduplication, and tokenizer training. This stage converts and normalizes — it does not discard documents.

**Retention impact:** ~100% (with a quarantine lane for uncertain conversions).

### 5.1 Bijoy Encoding Detection & Conversion — Evidence-Based

Legacy Bangladeshi legal, institutional, and journalistic text was produced in Bijoy encoding — a proprietary non-Unicode standard still common in government archives and court records. Bijoy-encoded bytes passed through a UTF-8 pipeline do not raise exceptions; they silently produce garbage codepoints that survive all language filters.

**v2 heuristic (retained as one signal only):** detect via byte-range heuristics (>70% of bytes in 0x80–0xFF with characteristic Bijoy signatures).

**v3.0 correction — a byte-range heuristic alone will corrupt valid Unicode Bengali,** which also lives entirely above 0x80. Conversion must be *decided by evidence, applied reversibly, and verified*:

1. **Prefer declared encoding** from the source registry where known.
2. **Generate candidates**: leave as-is, Bijoy→Unicode map, and any other legacy map the source suggests.
3. **Score each candidate** on: Bengali character validity, grapheme-cluster well-formedness, ratio of valid conjuncts, dictionary hit rate, and provisional KenLM/character-LM score.
4. **Accept the best candidate only if it beats "leave as-is" by a calibrated margin.**
5. **Quarantine** when no candidate wins clearly.
6. **Log the transformation** (`encoding_operations`) with enough information to reverse it, and retain the original bytes for the golden samples.

Tag all converted documents `"encoding": "bijoy_converted"` in the provenance record. Validate against a manually verified 1% sample **before** processing at scale; the Legal Documents source (876 `.txt` files) is the highest-probability Bijoy source and is the calibration exercise. Tier 1 backbone sources (FineWeb2, HPLT, Sangraha) are already clean Unicode and skip Bijoy detection entirely.

### 5.2 Unicode NFC Normalization

Bengali has multiple valid Unicode representations for the same visible glyph due to different orderings of vowel matras, hasanta (virama), and nukta. These are semantically identical but lexicographically different.

```python
import unicodedata
text = unicodedata.normalize('NFC', text)
```

Applied to every document and chunk before any string comparison, hash computation, or language model scoring. Without NFC normalization, MinHash LSH treats identical texts as different (causing deduplication misses) and BPE tokenizers learn spurious sub-word units for the same grapheme cluster. **This is a prerequisite for the global dedup pass in Stage 2 to work correctly across sources.**

The normalization form is recorded in the schema (`processing_version`) so that a future change to normalization invalidates dedup results explicitly rather than silently.

### 5.3 ZWNJ Canonicalization

Bengali has linguistically valid uses of the zero-width non-joiner (ZWNJ, U+200C) to force explicit *juktakkhor* decomposition. Web-scraped text contains many spurious ZWNJs inserted by font-rendering workarounds.

```python
import re
# Preserve ZWNJ only where it follows a virama (U+09CD)
text = re.sub(r'(?<!\u09CD)\u200C', '', text)
```

**v3.0 note:** validate this rule on the golden samples before global application, and count how many ZWNJs it removes per source. A source with an anomalous removal rate indicates a font-conversion pathology worth handling explicitly rather than deleting.

### 5.4 Private Use Area & Variation Selector Stripping

Strip Unicode Private Use Area codepoints (U+E000–U+F8FF) and variation selectors (U+FE00–U+FE0F) originating from proprietary font conversions.

```python
text = re.sub(r'[\uE000-\uF8FF\uFE00-\uFE0F]', '', text)
```

**v3.0 caution:** PUA codepoints are frequently the *only* carrier of meaning in font-hacked Bengali text. Before stripping, count PUA density per source: a document that is largely PUA is a **conversion candidate**, not a stripping candidate. Route high-PUA documents to the Section 5.1 candidate-scoring path instead of silently emptying them.

### 5.5 Source-Specific Format Extraction & Unified Schema

Each source requires format-specific extraction before entering the unified pipeline. All sources are normalized to a **single unified schema** so the global dedup pass can operate across them.

| Source | Extraction Strategy |
|--------|-------------------|
| FineWeb2 / HPLT / MADLAD (`.jsonl.zst` / Parquet) | Read `text` field; preserve publisher quality metadata |
| Sangraha | Read text; tag romanized vs. native script; tag synthetic subset |
| mC4 (`.txt` parts) | Split on double newlines |
| Newspaper JSON (ebD) | Parse JSON; extract article body; preserve metadata |
| IndicCorpV2 / CC-100 (monolith `.txt`) | Reconstruct documents from blank-line boundaries |
| Bengali Wikipedia (`.arrow`) | Read via PyArrow; extract `text` column |
| OCR'd NCTB / Legal (`.txt`) | Direct ingestion (Legal: Bijoy priority) |
| Banglapedia | Per-article files; one file = one document |
| Samanantar | Extract Bengali side only |

**v3.0 expanded unified schema.** v2's `{doc_id, text, source_dataset, source_url, license}` is insufficient to support licensing views, dedup clusters, chunk lineage, and auditing. The full schema:

```text
# Identity
doc_id                  # stable identity of the SOURCE RECORD
content_id              # hash of normalized CONTENT (identity != equality)
text_hash               # SHA-256 of normalized text

# Provenance
source_id
source_version
source_record_id
source_url
timestamp
license
release_class           # redistributable | train-only | excluded
tier
source_priority

# Language / script
language_scores         # full distribution, not just argmax
script_statistics       # per-script character ratios
encoding_operations     # ordered list of applied transformations

# Chunk lineage
parent_doc_id
chunk_index
char_span               # offsets into the parent document
discontinuity_before    # bool: preceding chunk was dropped

# Scores and labels (distributions, not hard labels)
quality_scores          # {classifier: score}
kenlm_scores            # {model_id: logprob, percentile}
domain_distribution
dialect_distribution
difficulty_score
curriculum_tier

# Safety / policy
pii_flags
safety_flags
synthetic_origin        # none | llm_rescue | source_synthetic

# Dedup
dedup_cluster_id
canonical_doc_id
cluster_source_ids      # ALL sources that contributed this content

# Reproducibility
processing_version      # config hash + code commit + model hashes
```

Store **probability distributions**, not only argmax labels — a hard label discards the information needed to re-tune a threshold without re-running the classifier.

---

## 6. Stage 1 — Document-Level Pre-screening

**Purpose:** A fast, inexpensive coarse pass eliminating obviously unusable documents before deduplication and expensive per-chunk processing. Thresholds are deliberately conservative — borderline documents are passed to later chunk-level rescue.

**Retention target:** ≥90% `HYPOTHESIS`

### 6.1 From Single Thresholds to a Composite Routing Score

v2 gated on a single signal: fastText LID **>70% Bengali**, intentionally lower than TituLLMs' 95% (Nahin et al., 2025) because 95% discards code-mixed social media, technical blogs, and bilingual educational material. That rationale stands and is retained.

**v3.0 correction:** a single document-level LID threshold is still unreliable for romanized Bengali (fastText frequently labels it English or Hindi), OCR text, table-heavy documents, short documents, and multilingual educational content. Replace the single gate with a **composite routing score** over signals that are cheap to compute:

| Signal | Role |
|--------|------|
| fastText LID probability (`lid.176.bin`), full distribution | Primary language evidence |
| Bengali script character ratio | Catches LID failures on short text |
| Unicode/grapheme validity rate | Detects encoding damage |
| Alphabetic-token ratio | Detects tables and number dumps |
| Line-level language distribution | Detects mixed-language documents |
| Document length | Modulates confidence in all of the above |
| Source prior (tier, publisher scores) | Trusts Tier 1 more than own WET |

Routing:

| Composite outcome | Route |
|-------------------|-------|
| Clear Bengali | **PASS** |
| Clear non-Bengali / clear garbage | **REJECT** (with reason code) |
| Ambiguous, romanized, OCR-suspect, or short | **QUARANTINE** → review or chunk-level rescue |

> Tier 1 backbone sources already carry publisher language scores; their LID can be trusted and this step used only as a light sanity check.

The real quality gate operates at chunk granularity in Stage 5 (>85% Bengali confidence on the clean path), which is what makes conservative document-level screening the correct choice.

### 6.2 Document Length Filter `HYPOTHESIS`

| Condition | Action | Rationale |
|-----------|--------|-----------|
| < 50 tokens | Drop | Insufficient content for any training signal |
| > 200,000 tokens | Quarantine (not drop) | Likely a concatenation artifact — but may be a legitimate book or statute |
| 50–200,000 tokens | Pass | Legal clauses and poem stanzas are often 50–100 tokens |

**v3.0 changes:** "tokens" here means **Unicode word count**, not tokenizer tokens (the tokenizer does not exist until Stage 10) — the unit must be stated because a 512-"token" chunk means something different under each. Over-length documents are quarantined and splittable rather than deleted; a concatenated file is a *parser* bug to fix, not data to discard.

### 6.3 Structural Noise Ratio Filter `HYPOTHESIS`

| Signal | Threshold | Rationale |
|--------|-----------|-----------|
| URL density (URLs / total tokens) | > 20% | Spam and link-farm content |
| Punctuation + symbol density | > 35% | Malformed OCR or encoding artifacts |
| Number density | > 50% | Tables scraped as text |
| Repeated line ratio (line seen > 3×) | > 30% | Boilerplate and navigation menus |
| Average line length | < 3 tokens | Navigation fragments (poetry preserved) |

**v3.0 calibration requirement:** every row above must be validated against the golden samples per source before it is allowed to delete. Known false-positive classes to protect: poetry (short lines), legal text (high number density from clause numbering), statistical/sports reporting (high number density), reference lists, and OCR'd tables of contents. Where a rule fires on a protected class, prefer quarantine.

### 6.4 Coarse PII Scan

Fast regex scan for Bangladesh-specific PII:

- Bangladesh NID patterns: `\d{10}`, `\d{13}`, `\d{17}`
- Phone numbers: `(\+880|880|0)[- ]?\d{9,11}`
- Email addresses: standard RFC 5322 regex
- Bank account numbers: common Bangladeshi bank formats

Documents exceeding 5 PII instances per 1,000 tokens are **flagged for redaction at Stage 8 — not dropped**. PII is a redaction problem, not a disqualification event.

**v3.0 note:** the bare `\d{10}`/`\d{13}`/`\d{17}` patterns will match years, prices, phone numbers, sports statistics, and legal citation numbers. At this stage they are *flags only*, never edits; all actual redaction happens under the Section 13.2 policy with contextual validation.

### 6.5 Retention Anomaly Gate

At Stage 1 completion, compute retention. If it falls below 85%, the pipeline **halts** and requires a threshold audit before proceeding. Sub-85% retention at coarse screening indicates a systemic problem with the source data or thresholds, not with individual documents.

**v3.0 refinements:** the gate is evaluated **per source**, not globally (a single broken parser hides behind a healthy aggregate), and in **both document-weighted and token-weighted** terms. Retention is a *monitoring signal, not a quality objective*: a genuinely broken source should retain far less than 85%, and the correct response is to fix or exclude the source, not to loosen thresholds until the number looks acceptable.

```json
{
  "stage": 1,
  "run_id": "2026-02-01T09:00Z-a41f9c",
  "config_hash": "sha256:...",
  "code_commit": "git:...",
  "documents_in": 1000000,
  "documents_passed": 920000,
  "documents_quarantined": 34000,
  "retention_rate_docs": 0.92,
  "retention_rate_tokens": 0.94,
  "per_source": { "own_wet_cc": { "retention_docs": 0.71, "retention_tokens": 0.65 } },
  "drop_breakdown": {
    "composite_non_bengali": 45000,
    "too_short": 12000,
    "noise_ratio": 22800
  }
}
```

---

## 7. Stage 2 — Global Deduplication (Early, External-Memory) `BEYOND-SOTA`

**Purpose:** Remove duplicate content across *all* sources, once, before any expensive processing. This is the pipeline's most important structural stage: it converts a pile of overlapping corpora into a real, countable set of unique documents, and prevents downstream compute from being spent on data that will be discarded.

**Why this moved from v1's Stage 5 to Stage 2.** In v1, deduplication happened *after* per-chunk KenLM scoring and *after* Qwen-72B translation of code-mixed chunks — the pipeline could spend 72B-parameter inference translating duplicated Banglish that was then deleted. Running dedup early eliminates this waste. FineWeb also showed dedup ordering materially affects final quality, not just cost.

**Why this is global, not per-file.** The v1 extractor hashed only *within a shard*, but the biggest source of duplication is *cross-source* (FineWeb2 ↔ HPLT ↔ mC4 ↔ CC-100 ↔ own WET all overlap heavily). Dedup must be a single pass over the entire merged, unified-schema corpus.

**Retention:** ~60–80% of documents, source-dependent `HYPOTHESIS` — the real figure is an *output* of this stage.

### 7.1 Exact Deduplication

Compute the **SHA-256 hash** of each Unicode-normalized, whitespace-collapsed document (`text_hash`). Group by hash; retain one instance. This O(n) pass eliminates verbatim copies — common in news wire syndication, legal boilerplate, and CC re-crawls — with zero tuning.

Implementation: emit `(text_hash, doc_id, source_priority, quality_signals)` to Parquet, then **sort externally by hash** and group. Never hold the hash set in RAM.

### 7.2 Fuzzy Deduplication — Why v2's In-Memory Plan Does Not Fit

v2 specified `datasketch` MinHash LSH with 256 permutations at a global Jaccard threshold of 0.85. The threshold and the global-single-threshold decision are correct and retained. **The in-memory implementation is not viable.**

The naive memory estimate counts only the signature arrays:

$$50\text{M docs} \times 256 \text{ perms} \times 4 \text{ bytes} \approx 51.2\ \text{GB}$$

It excludes Python object overhead, document identifiers, the LSH hash tables and buckets, the candidate-pair set, allocator fragmentation, OS page cache, I/O buffers, and the memory reserved for GPU work in the same 128 GB unified pool. In practice a Python `datasketch` index at this scale needs several times the estimate — it will not fit.

### 7.3 External-Memory Banded MinHash (v3.0 implementation)

Stage 2 becomes a **sort-and-group pipeline over Parquet**, with bounded memory at every step:

1. **Normalize** text (NFC, whitespace collapse, case/punctuation policy fixed and recorded).
2. **Shard** the corpus into fixed-size shards (target ~200k–500k documents each).
3. **Per shard**, compute `text_hash` and the MinHash signature; write signatures to Parquet/binary arrays. Shards are independent → parallel and resumable.
4. **Emit band keys**: for each document and each band, write `(band_id, band_hash, doc_id)`.
5. **Partition by `(band_id, band_hash)`** to disk.
6. **Sort/group each partition externally**; candidate pairs come from within-bucket combinations only. Cap bucket size and log oversized buckets (they indicate boilerplate, not duplication).
7. **Verify each candidate pair** against the actual shingle sets — do not trust the MinHash estimate alone.
8. **Union-find** over verified pairs to build clusters, processed partition-by-partition.
9. **Select the canonical document deterministically** (Section 7.4).

**Parameters to fix and record explicitly** — v2 left these implicit, and an undocumented change to any of them silently invalidates every dedup result:

| Parameter | Default | Note |
|-----------|---------|------|
| Shingle unit | word 5-grams | Character n-grams for very short text |
| Permutations | **128 (benchmark 128 vs 256)** | 256 doubles memory and I/O for often-marginal recall gain |
| Bands × rows | Chosen to match target Jaccard 0.85 | Record the exact (b, r) |
| Hash function + seed | Fixed, recorded | Reproducibility |
| Minimum document length for fuzzy dedup | ~50 words | Short text is unreliable under MinHash |
| Similarity verification | Exact Jaccard on shingle sets | Prevents false merges |
| Tie-breaking | Deterministic (Section 7.4) | Same input → same canonical choice |

### 7.4 Canonical Selection — Rights-Aware, Not Priority-Only

v2 chose the survivor by a fixed source-priority list:

```
priority: FineWeb2 > HPLT > Sangraha > Wikipedia/Banglapedia > curated news
          > IndicCorpV2 > MADLAD > CC-100 > mC4 > own WET crawls
```

This is a sound default but can pick a copy the project has **no right to redistribute**, or a truncated extraction over a complete one. v3.0 orders the decision:

1. **Redistribution rights** (`release_class`) — prefer a copy that keeps the document releasable
2. **Extraction quality / completeness** (length, boilerplate ratio, truncation markers)
3. **Metadata completeness** (URL, timestamp, publisher scores)
4. **Source authority** (the v2 priority list)
5. **Publication timestamp** (prefer the earliest original for news)
6. **Deterministic tie-break** (lowest `content_id`)

**Always retain `cluster_source_ids`** — the full list of sources that contributed identical content. This is what makes per-source overlap reporting and licensing audits possible after the fact.

### 7.5 Duplicate-Preservation Carve-Outs `NEW in v3.0`

Aggressive dedup destroys content classes where repetition is *the signal*. Exempt from fuzzy dedup (exact dedup still applies):

- dialectal variants of the same passage
- translations and parallel-corpus records (Samanantar)
- poems, songs, and refrains
- statutes, amendments, and their consolidated versions
- quoted canonical religious and literary text
- textbook exercises that legitimately repeat

### 7.6 Dedup Quality Validation Before Global Application

Build a **manually labelled pair set** (target 1,000+ pairs: exact duplicates, near-duplicates, same-topic-different-article, dialect variants, translations) and report **dedup precision and recall** at candidate thresholds before applying any threshold corpus-wide. Publish the chosen threshold with its measured precision/recall.

### 7.7 Dedup Statistics Report (required output)

```json
{
  "stage": 2,
  "run_id": "...",
  "config_hash": "sha256:...",
  "minhash": { "perms": 128, "bands": 16, "rows": 8, "target_jaccard": 0.85, "seed": 1337 },
  "validation": { "labelled_pairs": 1200, "precision": 0.0, "recall": 0.0 },
  "documents_in": 0,
  "documents_out": 0,
  "clusters": 0,
  "per_source": {
    "fineweb2":   {"in": 0, "unique_kept": 0, "removed_as_dup": 0, "net_new_pct": 0.0, "net_new_tokens_pct": 0.0},
    "hplt_v2":    {"in": 0, "unique_kept": 0, "removed_as_dup": 0, "net_new_pct": 0.0, "net_new_tokens_pct": 0.0},
    "own_wet_cc": {"in": 0, "unique_kept": 0, "removed_as_dup": 0, "net_new_pct": 0.0, "net_new_tokens_pct": 0.0}
  },
  "estimated_unique_tokens": 0,
  "estimated_unique_tokens_releasable": 0
}
```

> **Expectation:** own WET crawls, mC4, and CC-100 will show low `net_new_pct` after deduping against FineWeb2/HPLT (largely the same underlying CC pages). This is expected and fine — they remain useful for recall and source-priority fallbacks. Report `net_new_tokens_pct` alongside the document figure; a source can add few documents but many tokens, or the reverse.

---

## 8. Stage 3 — Model-Based Quality Scoring `SOTA`

**Purpose:** Score every surviving document with a lightweight learned quality classifier. This is the single most impactful modern data-quality technique (used by FineWeb-Edu and DCLM) and was absent from v1, which relied entirely on KenLM perplexity plus hand-tuned density heuristics.

**Philosophy alignment:** the classifier is a **scorer and ranker, not a hard filter.** Every document keeps its score; low-scoring documents are down-weighted in the mixing ratios (Stage 12) and sorted into harder curriculum tiers (Stage 11), but are not automatically discarded.

**Retention:** ~100% (scoring only).

### 8.1 Classifier Design

- **Model:** fastText supervised classifier (fast enough to score the entire corpus on CPU), or a small linear head on multilingual embeddings if higher accuracy is needed.
- **Output:** a continuous `quality_score` in [0, 1] per document, stored with the model ID and version.

### 8.2 Training Data — Source-Leakage Controls `NEW in v3.0`

v2's recipe was: positives = Wikipedia / Banglapedia / NCTB / curated news / FineWeb2 top bucket; negatives = raw low-LID web crawl, SEO spam, boilerplate WET, garbled text.

**The problem:** those positive and negative pools differ by *source*, not only by *quality*. A classifier trained this way learns "does this look like Wikipedia?" — it will down-rank good blog posts, good forum answers, and good OCR'd books simply because they are not Wikipedia-shaped, which is precisely the retention failure this project exists to avoid.

Corrections:

1. **Human-labelled examples sampled across every source**, including good *and* bad examples **from the same source**.
2. **Split train/validation/test by domain and hostname**, so the model cannot memorize a publisher.
3. **Measure annotator agreement** on a shared subset; a classifier is only as good as its label noise.
4. **Keep an explicit uncertainty band** in the middle of the score range; documents there are ranked neutrally, not penalized.
5. **Publish a model card and dataset card** with the label rubric.

**Evaluation:** ROC-AUC, PR-AUC, calibration error (ECE / reliability curve), ranking NDCG, and **per-subgroup false-positive rates** by source, domain, dialect, and length. "Wikipedia scores higher than raw WET" is *not* validation — it is the leakage restated.

### 8.3 How the Score Is Used Downstream

| Consumer | Use of `quality_score` |
|----------|------------------------|
| Stage 11 (curriculum) | Low score → later/harder tier; high score → foundation tier |
| Stage 12 (mixing) | Mixing weights lean toward high-score documents |
| Own WET crawls | Expected to score low on average; kept but down-weighted, so the backbone dominates |
| Ablations | `abl-no-quality-classifier`, `abl-quality-hard-filter` (Section 22) |

### 8.4 Why This Replaces Most of the KenLM Fleet's Burden

v1 used **8 domain-specific KenLM models** plus a BanglaBERT domain classifier as the primary quality mechanism — many coupled components where a single misclassification cascades into false drops. From v2 onward:

- The **learned quality classifier** is the primary quality signal.
- **KenLM is retained** for perplexity-based *chunk* scoring (Stage 5) and curriculum difficulty (Stage 11), but the 8-model fleet is **optional and ablation-gated**. A single robust KenLM trained on Wikipedia + books + curated news is the default; the fleet must earn its complexity in an ablation.

---

## 9. Stage 4 — Domain & Dialect Classification `NOVEL`

**Purpose:** Assign domain and dialect labels to every document. These labels govern KenLM model selection (Stage 5), chunk-level dedup thresholds (Stage 7), curriculum difficulty (Stage 11), and final mixing ratios (Stage 12).

**Retention impact:** ~100%. This stage classifies — it does not filter.

### 9.1 Tiered Delivery `NEW in v3.0`

v2 required ~10,000 human-labelled domain documents and ~5,000 dialect samples. That is potentially the single largest manual bottleneck in the project, and source-derived labels ("everything from Prothom Alo is `news`") are **weak supervision, not ground truth**. Deliver in three levels so the pipeline never blocks on annotation:

| Level | Mechanism | Blocking? |
|-------|-----------|-----------|
| **MVP** | Source metadata + rules + keyword priors, emitting a **distribution** with an `unknown` option | No — ships immediately |
| **Enhanced** | Small manually labelled *evaluation* sets (500–1,000 per task) + a lightweight classifier | No |
| **Research** | Full BanglaBERT domain/dialect classifiers at v2's label volumes | Only after downstream value is shown |

### 9.2 Domain Taxonomy

Architecture (Research level): **BanglaBERT-based multi-class classifier**, fine-tuned on ~10,000 human-labelled documents.

| Domain ID | Label | Representative Sources |
|-----------|-------|----------------------|
| `news` | News & Journalism | Prothom Alo, bdnews24, ebD, Potrika, 1000 Day News |
| `legal` | Legal & Constitutional | Legal Documents, BD Laws |
| `educational` | Curriculum & Educational | NCTB Books, Old HSC materials |
| `literature` | Literature & Fiction | Poem Dataset, literary web content |
| `religious` | Religious Texts | Quran translations, hadiths, puja texts |
| `web_general` | General Web | FineWeb2, HPLT, mC4, CC-100, IndicCorpV2 (default bucket) |
| `social` | Social Media & Informal | Comments, posts, chat excerpts; Sangraha romanized |
| `medical` | Medical & Health | Medical Dataset, DGHS publications |
| `technical` | Technical & Scientific | Engineering and computing articles |
| `encyclopedic` | Encyclopedic Reference | Bengali Wikipedia, Banglapedia |

**Confidence gate:** softmax maximum < 0.6 → `web_general` default; all low-confidence classifications logged. **v3.0:** store the full `domain_distribution`, and prefer `unknown` over `web_general` when the distribution is flat — defaulting ambiguity into `web_general` corrupts the mixing-ratio audit in Section 17.2.

### 9.3 Dialect Detection Layer `NOVEL`

A secondary **BanglaBERT-based dialect classifier** (~5,000 labelled samples) detects regional variation.

| Dialect ID | Region | Approximate L1 Speakers |
|------------|--------|------------------------|
| `shuddho` | Standard Bengali (Kolkata/Dhaka norm) | ~170M |
| `chattogram` | Chittagong Division | ~16M |
| `sylhet` | Sylhet Division | ~11M |
| `barishal` | Barisal Division | ~8M |
| `rangpur` | Rangpur Division | ~15M |
| `noakhali` | Noakhali / Comilla region | ~9M |

**v2 logic:** confidence > 0.70 for a non-`shuddho` category → assign it; otherwise default to `shuddho`.

**v3.0 correction — do not default to `shuddho`.** Use `unknown`. Defaulting every uncertain document to standard Bengali guarantees the Section 17.2 audit reports ~95% `shuddho` regardless of what the corpus actually contains, which makes the dialect target unverifiable and the dialect contribution unmeasurable. Store the full `dialect_distribution`.

**Rationale (unchanged):** without dialect detection, dialectal text produces anomalously high perplexity against a standard-Bengali KenLM and is dropped as noise. Dialect awareness is a prerequisite for retaining this data and for serving all 230M+ Bengali speakers.

---

## 10. Stage 5 — Sub-Document Processing `BEYOND-SOTA`

**Purpose:** The core data rescue mechanism. Documents are split into chunks; each chunk is independently scored, routed, and either accepted, rescued, or dropped. A document containing 40% noise and 60% valid Bengali contributes its 60% — the noise is removed, not the document.

**Retention target:** ≥85% of chunks `HYPOTHESIS`

### 10.1 Semantic Chunking

Split each document using this priority order:

1. **Paragraph breaks** (`\n\n`) — primary boundary.
2. **Sentence boundary** (Bengali sentence tokenizer) — secondary split when a paragraph exceeds the size limit.
3. **Hard limit** as an absolute maximum. No sliding window — avoids double-counting.

**v3.0 corrections:**

- **Define the unit.** "512 tokens" is undefined before Stage 10. Use **Unicode word count** during cleaning, or a **frozen provisional tokenizer** whose version is recorded in `processing_version`. State which, and never mix the two.
- **Never hard-split mid-sentence** when a sentence boundary exists within a tolerance window; measure the sentence-fragmentation rate at the limit.
- **Preserve `char_span` offsets** into the parent document so any chunk can be traced back and re-derived.
- **Reconsider the 20-token floor** `HYPOTHESIS`. v2 discarded chunks below 20 tokens. Poem couplets, definitions, captions, legal sub-clauses, and dialogue turns are frequently shorter and frequently valuable. Make the floor domain-aware and quarantine rather than delete for `literature`, `legal`, and `educational`.

Tag each chunk with: parent document ID, position index, domain distribution, dialect distribution, document quality score, and `discontinuity_before`.

### 10.2 Per-Chunk Script Analysis & Routing

```python
def bengali_ratio(text):
    bengali = sum(1 for c in text if '\u0980' <= c <= '\u09FF')
    non_space = sum(1 for c in text if not c.isspace())
    return bengali / non_space if non_space > 0 else 0
```

| Bengali Script Ratio | Route |
|----------------------|-------|
| ≥ 0.80 | **CLEAN** → Section 10.3 (KenLM scoring) |
| 0.25 – 0.80 | **CODE-MIXED** → Stage 6 |
| < 0.25 | **DROP → QUARANTINE** — predominantly non-Bengali |

Additionally, run per-chunk fastText LID on the clean path with a **>85% Bengali confidence** threshold.

**v3.0 note:** the `< 0.25` bucket contains **romanized Bengali**, which has a Bengali script ratio of ~0. Sending it straight to DROP deletes exactly the register that Sangraha was added to supply. Route `< 0.25` to a **romanization check** first (Section 11.6); only then drop.

### 10.3 KenLM Perplexity — A Feature First, A Gate Only If Calibrated

**Default:** a single robust 5-gram KenLM trained on Wikipedia + Bengali books + curated post-2015 news scores clean chunks.

**Optional (ablation-gated):** v1's fleet (`kenlm_news`, `kenlm_legal`, `kenlm_educational`, `kenlm_literature`, `kenlm_encyclopedic`, `kenlm_web`, `kenlm_medical`, `kenlm_social`), selected by the Stage 4 domain label. Retained as `abl-multi-kenlm`.

**v2 gating logic (retained as the ablation-gated *gate* mode):**

```
For each clean chunk:

  1. Score perplexity against the (single or domain-matched) KenLM.

  IF perplexity <= P97 of the reference:
      -> ACCEPT

  ELSE IF perplexity <= P99:
      -> SECOND CHANCE: re-score against the permissive web KenLM.
        -> If web perplexity <= P95: ACCEPT (likely register/domain mismatch).
        -> Else: reroute to Stage 6 as code-mixed (rescue attempt).

  ELSE (perplexity > P99):
      -> QUARANTINE (v3.0: was DROP)

  IF perplexity < P3:
      -> FLAG for manual review (possible boilerplate).
```

**v3.0 default mode — score, do not gate.** Percentiles derived from a narrow reference corpus systematically penalize dialects, technical registers, archaic literature, OCR text, name/number-dense text, and code-mixed prose. Store `kenlm_scores` (logprob + percentile) as a **feature** consumed by the quality ranker (Stage 3) and curriculum difficulty (Stage 11). Enable the gate only when:

- percentiles are calibrated **per broad register**, not corpus-globally;
- every threshold is validated on labelled samples;
- high-value sources have explicit exceptions;
- extremes route to quarantine, not deletion;
- loss is reported **token-weighted and document-weighted**.

**Dialect exception (retained):** chunks tagged with any non-`shuddho` dialect — **or `unknown`** — are always scored against the most permissive (web) KenLM.

### 10.4 Document Survival Decision `HYPOTHESIS`

```
survival_ratio = surviving_chunks / total_chunks
```

| Survival Ratio | Action |
|----------------|--------|
| ≥ 0.50 | Reconstruct from surviving chunks |
| 0.20 – 0.50 | Reconstruct if surviving token count ≥ 150 tokens |
| < 0.20 | Drop entire document |

**v3.0 changes:** compute the ratio **token-weighted**, not chunk-weighted (a document whose one surviving chunk is 80% of its text should survive), and record `discontinuity_before` metadata.

**Do not insert literal `[...]` markers into the training text.** v1/v2 inserted `[...]` between non-adjacent surviving chunks. That injects a token sequence the model will learn to generate, and it pollutes tokenizer training. The *intent* — preventing false sentence continuity across dropped sections — is correct; implement it with metadata (`discontinuity_before: true`) plus a paragraph break, and let the training-time packer decide whether to emit a separator.

---

## 11. Stage 6 — Dual-Path Code-Mixed Processing `NOVEL`

**Purpose:** Handle chunks containing a mixture of Bengali and English text (Bengali script ratio 0.25–0.80).

**Rationale:** Real Bengali internet users write extensively in code-mixed "Banglish." BnSentMix (2025) and MixSarc (2026) demonstrate that code-mixed text is a valuable training signal. A model trained exclusively on monolingual Bengali will fail on a significant fraction of real-world input.

### 11.1 LLM Translation Is Optional and Restricted

v1 routed 70% of code-mixed chunks through **Qwen-72B translation**. v2 reversed this because (a) translating millions of chunks with a 72B model is an enormous compute cost, and (b) large-scale synthetic rescue carries model-collapse / distribution-shift risk (Shumailov et al., 2024). v3.0 keeps the v2 position.

**Default:** **Path B — Native Preservation** for all viable code-mixed chunks.

**Optional (ablation-gated):** **Path A — LLM Rescue** on a small high-value subset (e.g. `news`/`educational` code-mixed chunks with Bengali ratio 0.50–0.80), using a smaller/faster model than 72B. Controlled by `abl-rescue-restricted` and `abl-rescue-off`.

Because dedup runs at Stage 2, no translation compute is ever spent on duplicated chunks.

### 11.2 Path A — Translation (when enabled)

**Pre-screening:** remove only pure noise before translation (stop-word ratio > 90%, emoji/hashtag-only content, keyword lists without sentence structure).

**Engine:** a locally-served instruction model (Qwen-family, Apache 2.0) via vLLM with continuous batching.

```
System prompt:

You are a professional Bengali language translator and editor.
Translate the following code-mixed Bangla-English text into pure, fluent, formal Bengali prose.

Rules:
1. Preserve ALL factual content: names, dates, numbers, places, organizations.
2. Do NOT add any new information not present in the original.
3. Do NOT summarize - translate the full content.
4. Output ONLY the translated Bengali text. No commentary.

Input:
{chunk_text}
```

**v3.0 provenance requirement:** record `model_id`, prompt hash, decoding parameters (temperature, top-p, seed, max tokens), and the source→output linkage for every generated span. Synthetic text without these records cannot be audited, capped, or excluded later — and **the original chunk is always retained**, never replaced.

### 11.3 Path A — Three-Gate Quality Validation (when enabled) `HYPOTHESIS`

- **Gate A — Named Entity Preservation:** `preserved_entities / original_entities` ≥ **0.75** (XLM-RoBERTa NER)
- **Gate B — KenLM Perplexity:** below **P85** of the reference model
- **Gate C — Length Ratio:** translation between **0.60×** and **2.0×** the original token count

**Second-chance rescue:** chunks failing Gate B but passing A and C are re-scored against the permissive web KenLM (accept if ≤ P90); otherwise the *original* code-mixed chunk is rerouted to Path B.

**v3.0 addition:** validate all three gate constants on a human-rated sample of translations before trusting them, and add a **hallucination check** (facts/numbers present in output but absent from input) — Gate A catches dropped entities but not invented ones.

### 11.4 Path B — Native Code-Mixed Preservation `NOVEL` `HYPOTHESIS`

Chunks retained in original code-mixed form. v2 gates:

1. **Minimum structure:** ≥ 2 complete sentences
2. **Content density:** ≥ 10 unique non-stop content words
3. **Safety filter:** BanglaBERT safety classifier

**v3.0 corrections:**

- The **two-complete-sentences** rule rejects natural short conversational Banglish — the exact register this path exists to preserve. Lower it for `social`, or replace it with a coherence check.
- **Do not apply a stricter safety threshold to code-mixed text than to monolingual text.** v2 justified elevated strictness by "elevated baseline toxicity"; that is a demographic penalty on informal registers, not a safety improvement. Use the same Section 13.3 policy everywhere.

### 11.5 Provenance & Corpus Caps

Tag all code-mixed output with `source_type` (`synthetic_rescue` | `native_code_mixed`), original Bengali ratio, and path taken.

**Corpus caps** (enforced at Stage 12):
- Synthetic rescue content: **≤ 15%** of the final corpus (model-collapse guard)
- Native code-mixed content: **≤ 8%** of the final corpus

**v3.0:** the synthetic cap counts **all** synthetic text, including Sangraha's synthetic portion — not just our own LLM output.

### 11.6 Code-Mixed Taxonomy `NEW in v3.0`

A single "code-mixed" bucket conflates registers that need different handling. Classify into:

| Category | Definition | Default handling |
|----------|-----------|------------------|
| `native_code_mixed` | Bengali script with embedded English | Preserve (Path B) |
| `romanized_bengali` | Bengali language in Latin script | **Preserve** — rescued from the `< 0.25` drop bucket |
| `english_in_bengali` | Predominantly Bengali doc with English quotes/terms | Preserve |
| `transliterated_bengali` | Mechanical transliteration | Preserve, tag distinctly |
| `multilingual_non_bengali` | Not substantially Bengali | Drop |

Romanized Bengali is the highest-value rescue in this stage: Sangraha supplies it, real users write it, and Section 10.2's script-ratio rule would otherwise delete all of it.

---

## 12. Stage 7 — Chunk-Level Deduplication

**Purpose:** A finer deduplication pass at *chunk* granularity with **domain-aware thresholds**. Global document dedup already happened at Stage 2; this pass catches duplicated chunks inside otherwise-unique documents (a boilerplate paragraph repeated across many articles) and deduplicates rescued/preserved code-mixed chunks.

### 12.1 Ordered, Cheap-First Design `NEW in v3.0`

Re-running full MinHash LSH over *every chunk* is the most expensive operation in the pipeline (chunk count ≫ document count) and the most destructive. Run in this order and stop as soon as the marginal benefit disappears:

1. **Exact chunk dedup** (`content_id`) — cheap, safe, removes most of the volume
2. **Repeated-line and boilerplate fingerprinting** — the actual target ("Follow us on Facebook", cookie notices, bylines)
3. **Host/template-level boilerplate removal** — lines recurring across many documents from one host
4. **Fuzzy chunk dedup** — only for eligible domains, only if steps 1–3 leave meaningful duplication
5. **Carve-outs** — canonical legal, religious, and literary text keeps repetition (Section 7.5)

### 12.2 Domain-Calibrated MinHash LSH `HYPOTHESIS`

`datasketch`-style banded MinHash (same external-memory implementation as Stage 2), with domain-specific Jaccard thresholds:

| Domain | Jaccard Threshold | Rationale |
|--------|-------------------|-----------|
| `news` | 0.80 | Wire service syndication |
| `social` | 0.80 | Copy-pasted comments |
| `web_general` | 0.82 | CMS boilerplate |
| `technical` | 0.85 | Standard terminology |
| `encyclopedic` | 0.88 | Shared reference content |
| `educational` | 0.88 | Textbook repetition |
| `medical` | 0.88 | Repetitive clinical language |
| `religious` | 0.93 | Canonical versional variation |
| `legal` | 0.95 | Statutory near-exact repetition |
| `literature` | 0.97 | Only verbatim copies removed |

**Every value in this table is a hypothesis, not a constant.** Validate each on the labelled pair set (Section 7.6) restricted to that domain before it deletes anything. Where a domain has too few labelled pairs to validate, fall back to exact + boilerplate dedup only.

### 12.3 Code-Mixed Deduplication

Native code-mixed chunks (Path B) are deduplicated separately at **Jaccard 0.80** — Banglish social content has exceptionally high copy-paste rates. `HYPOTHESIS`

---

## 13. Stage 8 — Final Quality Control

**Purpose:** PII handling, content safety, document reassembly, and final length validation.

**Retention target:** ≥95% `HYPOTHESIS`

### 13.1 Document Reassembly

Reconstruct documents from surviving chunks (clean + rescue + native code-mixed). Preserve `discontinuity_before` metadata and paragraph breaks instead of inserting literal `[...]` markers (Section 10.4). Apply a final document-level KenLM pass at P93 perplexity **in scoring mode by default**; gating requires the Section 10.3 calibration conditions.

### 13.2 PII — A Policy, Not A Regex Sweep

v2 specified regex redaction (NID, +880 mobile, email, bank, passport) plus **NER-based person-name and street-address redaction**, replacing matches with typed placeholders (`[PHONE]`, `[NID]`, `[EMAIL]`, `[ADDRESS]`).

The placeholder approach is right. **Blanket person-name redaction is not** — it would destroy news, history, biography, and encyclopedic content, which is most of the high-value corpus. A model that has never seen "শেখ মুজিবুর রহমান" cannot discuss Bangladeshi history.

**v3.0 policy tiers:**

| Category | Action |
|----------|--------|
| Public figures, organizations, place names | **Keep** |
| Authors, bylines, public officials in role | **Keep** |
| Direct contact details (phone, email, personal address) | **Redact** → typed placeholder |
| Government/sensitive identifiers (NID, passport, bank, TIN) | **Redact** |
| Private individuals in incidental context (victims, commenters, minors) | **Redact or quarantine** |
| Already-public institutional addresses | **Keep** |

Implementation requirements: contextual validation before any numeric redaction (a 13-digit match may be a year range or a price); redaction *counts and rates* logged per source; a sampled human review of redactions before scale-up; and structure-preserving placeholders so sentences stay grammatical.

### 13.3 Content Safety Filtering

v2: Bengali keyword blocklist (hate speech, communal slurs, explicit content, violent incitement) → BanglaBERT safety classifier on keyword-triggered documents → drop at confidence > **0.75** (deliberately elevated to protect legitimate news and legal text).

**v3.0 refinement — separate categories rather than one "harmful" label**, because a single classifier at one threshold will delete journalism, history, court decisions, and counter-speech:

| Category | Action |
|----------|--------|
| Illegal / exploitative content (esp. CSAM) | **Remove unconditionally**, no threshold debate |
| Explicit sexual content | Remove or cap by policy |
| Hateful advocacy (author's own voice) | Remove |
| Quoted / reported speech in journalism | **Keep** — reporting is not advocacy |
| Historical and legal discussion of atrocities | **Keep** |
| Counter-speech and criticism of hate | **Keep** |

Require: context-aware classification (who is speaking), per-category thresholds validated on labelled Bengali data, measured false-positive rates on news and legal text, and quarantine for ambiguous cases.

### 13.4 Final Length Validation `HYPOTHESIS`

Drop documents below **150 tokens** after all processing — with domain exceptions for `literature` (poems), `legal` (clauses), and definitional `encyclopedic` entries, which route to quarantine instead.

### 13.5 Synthetic Proportion Audit

Documents where >50% of tokens originate from LLM rescue → flag and sample 200 for manual human review before inclusion. Track the corpus-wide synthetic fraction continuously against the 15% cap, counting source-synthetic (Sangraha) alongside our own rescue output.

---

## 14. Stage 9 — Evaluation Set Decontamination `SOTA`

**Purpose:** Prevent evaluation-set leakage into the pre-training corpus. Follows GPT-3 (Brown et al., 2020) and RefinedWeb (Penedo et al., 2023).

**Retention target:** ≥98% `HYPOTHESIS`

### 14.1 Blocklist — Frozen at Stage −1

The evaluation registry from Section 4.3 is the blocklist source. **Held-out splits are reserved at Stage −1**, before any filtering or classifier training, not "before decontamination" as in v2.

### 14.2 Multi-Signal Contamination Detection

v2 used one rule: drop a document sharing **≥ 3 unique 13-grams** with the blocklist. This is a reasonable primary signal but insufficient alone — it misses paraphrase and translation contamination, and in a morphologically rich language it can also fire on formulaic phrasing.

v3.0 combines signals and records which fired:

| Signal | Catches |
|--------|---------|
| Normalized exact match | Verbatim copies |
| **13-gram overlap ≥ 3 unique** (v2 rule, retained) | Near-verbatim |
| MinHash document similarity vs. eval items | Reworded copies |
| **Field-wise matching**: prompt and answer separately | Answer-key leakage where the question was rephrased |
| Translated / paraphrased checks where practical | Cross-lingual leakage (esp. translated benchmarks) |
| **Benchmark-family holdout** | Sibling-set leakage |

**Required output:** a **per-benchmark contamination report** — how many documents matched, by which signal, from which sources. Publish it with the corpus; contamination that is not reported is contamination that will be discovered by a reviewer.

---

## 15. Stage 10 — Tokenizer Co-Design & Fertility Validation `NOVEL`

**Purpose:** Train a Bengali-optimized tokenizer and validate it against the corpus *before* model training. The tokenizer is a first-class pipeline output.

**Rationale:** BengaliBPE (2025) showed morphology-aware merges produce better subword units; banglaLlama achieved 2.1× compression over LLaMA's tokenizer; IndicSuperTokenizer showed up to 39.5% fertility improvement with language-specific pre-tokenization.

### 15.1 Tokenizer Architecture

**Type:** Morphology-aware BPE via SentencePiece with Bengali-specific modifications.

- **Grapheme-cluster-aware initialization:** seed the vocabulary with complete Bengali grapheme clusters (base consonant + matra + optional virama). Never split a *juktakkhor* mid-cluster — "ক্ষ" is one token, not "ক" + "্" + "ষ".
- **Morphology-aware merge priority:** prioritize merges producing meaningful Bengali units — verb inflections (`-ছে`, `-ছিল`, `-বে`, `-তে`), case markers (`-র`, `-ের`, `-কে`), postpositions (`থেকে`, `পর্যন্ত`, `দিয়ে`), derivational suffixes (`-কারী`, `-ময়`, `-শীল`).
- **Vocabulary size:** 64,000 tokens `HYPOTHESIS`
- **Character coverage:** 0.9999

**v3.0 implementation warning.** Stock SentencePiece provides **neither** morphology-prioritized merges **nor** guaranteed grapheme-cluster safety. Both properties must be *engineered and verified*, typically via grapheme-aware pre-tokenization plus a custom seed vocabulary and merge-scoring pass, then **measured** — never assumed from the config. If they cannot be delivered, say so and fall back to the best measured candidate rather than claiming a property the tokenizer does not have.

### 15.2 Candidate Sweep Before Committing `NEW in v3.0`

Do not fix vocabulary size or algorithm in advance. Train and compare:

- **Unigram:** 32K, 48K, 64K
- **BPE:** 32K, 48K, 64K
- **Byte fallback:** on / off
- **Normalization variants:** NFC only vs. NFC + Bengali-specific rules
- **Grapheme-safe pre-tokenization:** on / off

### 15.3 Fertility Benchmarking Gate `NOVEL` `HYPOTHESIS`

Measure **fertility** (tokens per word) across domains and dialects:

| Benchmark | Target (tokens/word) | Failure Threshold |
|-----------|---------------------|-------------------|
| Standard Bengali news | ≤ 1.8 | > 2.5 |
| Bengali literature (prose) | ≤ 2.0 | > 2.8 |
| Bengali legal text | ≤ 2.2 | > 3.0 |
| Code-mixed Banglish | ≤ 2.5 | > 3.5 |
| Dialectal Bengali | ≤ 2.3 | > 3.2 |

If any domain exceeds its failure threshold, retrain with adjusted merge weights. Benchmark against vanilla SentencePiece 64K, Llama-3.2, and (where available) BengaliBPE / IndicSuperTokenizer / TituLLMs tokenizers.

**v3.0 additional metrics** — fertility alone does not identify the best tokenizer:

- unknown / byte-fallback rate
- sequence-length distribution (not just the mean)
- **grapheme fragmentation rate** (how often a cluster is split — this is the actual claim being made)
- morphology-boundary alignment against a labelled affix set
- compression (bytes per token)
- **downstream validation loss from a small pilot model** — the deciding metric

Choose vocabulary size **empirically from this sweep**, not from a pre-committed 64K.

### 15.4 Corpus Re-Tokenization

After validation, re-tokenize the entire corpus to produce the definitive token count. All downstream metrics — domain distribution, caps, epoch planning, model size — are computed from this count, with the tokenizer hash recorded alongside.

---

## 16. Stage 11 — Curriculum Scoring & Training Manifest `NOVEL` `OPTIONAL in v3.0`

**Purpose:** Score every document by difficulty and produce a training-order manifest. First application of curriculum learning to Bengali LLM pre-training.

**Rationale:** Large-scale studies report reductions in training steps to baseline with curriculum-ordered data (ACL 2026); DUCL (AAAI 2026) argues ordering must consider both difficulty and utility.

### 16.1 Default Is a Shuffled Baseline `NEW in v3.0`

Strict easy-to-hard ordering carries real risks: it reduces sampling randomness, amplifies source bias within each tier, can cause distribution shift at tier boundaries, and rests on the assumption that **low quality equals high difficulty** — which is false (garbled OCR is low quality *and* uninformative, not "hard").

**Therefore:** the production default is **temperature-based quality/domain sampling with a global shuffle**. Curriculum ordering is computed, scored, and stored — but adopted only if a controlled pilot beats the baseline. Compare four arms:

1. Fully shuffled (baseline)
2. Quality-weighted sampling
3. Curriculum ordering (below)
4. Competence-based curriculum (pacing driven by model progress)

### 16.2 Difficulty Metrics

- **Compression Ratio (CR):** `len(utf8_bytes) / len(zlib.compress(utf8_bytes))`. Higher = more predictable = easier.
- **Lexical Diversity (MTLD):** mean tokens before running type-token ratio drops below 0.72. Higher = harder.
- **Quality & Perplexity:** the Stage 3 quality score and the Stage 5 KenLM perplexity rank, normalized **within domain**.

### 16.3 Composite Difficulty Score

```python
difficulty = (
    0.30 * normalized_inverse_CR +          # Low compression = harder
    0.30 * normalized_MTLD +                 # High lexical diversity = harder
    0.25 * normalized_domain_perplexity +    # High perplexity = harder
    0.15 * (1 - quality_score)               # Low quality = later/harder
)
```

The weights are `HYPOTHESIS`; treat them as an ablation axis, not settled values.

### 16.4 Curriculum Tiers

| Tier | Difficulty | Training Window | Content Profile |
|------|-----------|----------------|-----------------|
| **T1 — Foundation** | 0.00–0.25 | Steps 0–25% | Simple news wire, clean high-quality web. Core grammar and vocabulary. |
| **T2 — Expansion** | 0.25–0.50 | Steps 25–50% | Standard prose, educational, encyclopedic. Vocabulary breadth and knowledge. |
| **T3 — Enrichment** | 0.50–0.75 | Steps 50–75% | Literature, legal, medical, technical. Domain complexity. |
| **T4 — Mastery** | 0.75–1.00 | Steps 75–100% | Complex literary prose, code-mixed, dialectal text. Generalization. |

### 16.5 Soft Tier Transitions & Multi-Epoch Interaction `NOVEL`

Blend the next tier in at a 20% ratio during the final 10% of each tier's window to prevent abrupt distribution shift.

**Multi-epoch note:** because the plan allows repeated data (up to ~4 epochs, Section 17.1), tiers repeat each epoch. To avoid verbatim memorization, shuffle documents *within* each tier differently per epoch (recording the per-epoch seed) and keep the tier *sequence* fixed.

### 16.6 Training Manifest Output

A JSON Lines manifest specifying the exact training order: `doc_id`, `curriculum_tier`, `difficulty_score`, `quality_score`, `domain`, `dialect`, `source_type`, `release_class`, `token_count`, and the epoch shuffle seed. Documents shuffle within a tier; tier order is strict. The manifest also supports the shuffled baseline arm by emitting a single-tier ordering.

---

## 17. Stage 12 — Corpus Assembly, Model Sizing & Audit `BEYOND-SOTA`

### 17.1 Model Sizing — Data-Constrained, Not Chinchilla-Capped `NOVEL`

```python
unique_tokens      = sum(doc.token_count for doc in final_corpus)   # from Stage 10 re-tokenization
epochs             = min(validated_epochs, affordable_epochs_given_compute)
effective_tokens   = unique_tokens * epochs
# Choose the largest model that can be well-trained on effective_tokens
# given the secured training FLOPs, targeting an OVER-trained regime
# (>= 40-100+ tokens/param), NOT the Chinchilla 20:1 point.
```

**v3.0 change:** `epochs` is `validated_epochs` — the repetition count demonstrated non-degrading in the Section 17.6 pilot — capped by affordability. It is not a flat 4.

**Indicative sizing** (assuming ~4 epochs and an over-trained target; the Chinchilla column is shown only to illustrate how much *larger and better-trained* a model the same data supports once repetition is allowed):

| Unique Tokens | Effective (×4) | Chinchilla point (ref only) | Recommended (over-trained) | Indicative Architecture |
|---------------|----------------|-----------------------------|----------------------------|-------------------------|
| 10B | 40B | 500M | 350–500M, well-trained | 24L, 16H, 1024–1280D |
| 15B | 60B | 750M | 500–700M | 24–32L, 16–20H, 1280–1536D |
| 18B | 72B | 900M | 600–800M | 32L, 20H, 1536D |
| 25B+ | 100B+ | 1.25B | 800M–1B+ | 32L, 24H, 2048D |

> Recommended sizes are intentionally *below* the naive Chinchilla point for the effective token count, because an over-trained smaller model is a better, more useful, and cheaper-to-serve artifact than an under-trained larger one. Add unique data to move up this table — that is the real lever.

Compute budget for the run itself: `FLOPs ≈ 6 × params × effective_tokens`.

### 17.2 Corpus Composition Targets

| Metric | Target |
|--------|--------|
| Total clean tokens | Maximized — no fixed floor |
| Authentic Bengali (natural text) | ≥ 77% |
| Synthetic content (LLM rescue **+ source-synthetic**) | ≤ 15% |
| Native code-mixed content | ≤ 8% |
| High quality-score fraction (top tier) | Weighted up in mixing |
| Domain: `news` | 18–28% |
| Domain: `educational` + `encyclopedic` | 12–20% |
| Domain: `legal` | 5–14% |
| Domain: `literature` | 5–12% |
| Domain: `web_general` | 18–30% |
| Domain: `social` + `conversational` | 5–12% |
| Domain: `religious` | ≤ 5% |
| Domain: `medical` + `technical` | ≤ 8% combined |
| Dialect: `shuddho` | ≥ 85% |
| Dialect: non-standard (combined) | 5–10% |
| Dialect: `unknown` | Reported explicitly, not folded into `shuddho` |
| Curriculum: T1 / T2 / T3 / T4 | 20–30 / 25–35 / 20–30 / 10–20% |

All percentages are **token-weighted** and computed from the Stage 10 re-tokenization. Report each figure for both the **full training view** and the **releasable view** (Section 17.7).

### 17.3 Stage-by-Stage Retention & Dedup Report

```
+---------------------------------------------------------------------------+
|                    SHUDDHIKARAN v3 RETENTION REPORT                       |
|  run_id: ................  config_hash: ................                  |
+---------------------------------------------------------------------------+
| Stage    Description              docs%     tokens%    quarantined%       |
|  -1      Governance               n/a       n/a        n/a                |
|   0      Encoding repair          XXX.X     XXX.X      X.X                |
|   1      Pre-screening            XX.X      XX.X       X.X                |
|   2      GLOBAL dedup             XX.X      XX.X       X.X   <- unique    |
|   3      Quality scoring          100.0     100.0      0.0                |
|   4      Classification           100.0     100.0      0.0                |
|   5      Chunk processing         XX.X      XX.X       X.X                |
|   6      Code-mixed processing    XX.X      XX.X       X.X                |
|   7      Chunk dedup              XX.X      XX.X       X.X                |
|   8      Quality control          XX.X      XX.X       X.X                |
|   9      Decontamination          XX.X      XX.X       X.X                |
|                                                                           |
| UNIQUE TOKENS (train view):      ~XX.XB                                   |
| UNIQUE TOKENS (release view):    ~XX.XB                                   |
| EPOCHS VALIDATED:  X   -> EFFECTIVE TOKENS: ~XX.XB                        |
| MODEL:  XXXM params (over-trained)   CONFIG: XXL-XXH-XXXXD                |
+---------------------------------------------------------------------------+
```

Cumulative retention is reported from measurements only — never as a product of target rates.

### 17.4 Human Quality Audit

Before finalization: **500 documents**, stratified across domains, dialects, and source types, rated by **two native Bengali speakers** on fluency (1–5), factual plausibility (1–5), PII-freedom (pass/fail), safety (pass/fail), dialectal authenticity (non-`shuddho` only), and code-mixed naturalness (native code-mixed only).

Acceptance: mean fluency ≥ 4.0, mean plausibility ≥ 4.0, zero PII/safety failures.

**v3.0 additions:** report **inter-annotator agreement** (Cohen's κ) — two raters without an agreement statistic is an anecdote; adjudicate disagreements with a third rater; and audit a matched sample of **rejected and quarantined** documents, since the retention-first thesis is tested by what was wrongly discarded, not only by what was kept.

### 17.5 Provenance Sidecar

Every document carries a JSON Lines sidecar with source, license, `release_class`, domain and dialect distributions, encoding origin and operations, quality score, KenLM percentile, tokenizer fertility, difficulty score, curriculum tier, dedup cluster and contributing sources, synthetic origin, and the list of stages passed with their config hashes. This provenance — plus the release of the corpus itself — is the project's most citable contribution.

### 17.6 Pilot Validation Before the Full Run `NEW in v3.0`

Do not size and launch the full pretraining run from corpus statistics alone. Train small pilot models (e.g. 100–200M parameters on a few billion tokens) across competing corpus variants and measure:

- validation loss by source, domain, dialect, and benchmark
- **epoch degradation curve** (1 → 2 → 3 → 4 epochs) → this sets `validated_epochs`
- tokenizer candidate comparison (Section 15.3)
- curriculum arm comparison (Section 16.1)
- quality-classifier ablation (`abl-no-quality-classifier`, `abl-quality-hard-filter`)

Select the pipeline configuration from **model outcomes**, not retention percentages.

### 17.7 Dual Release Views `NEW in v3.0`

Materialize two views from one provenance store:

| View | Contents | Purpose |
|------|----------|---------|
| **Training view** | `redistributable` + `train-only` | Internal pretraining |
| **Release view** | `redistributable` only | Public corpus release |

Publish the release view with per-source attribution, the dedup and contamination reports, the audit results, and the exclusion register.

---

## 18. Engineering & Infrastructure

Storage and hardware decisions materially affect whether the pipeline finishes on time, so they are part of the specification.

### 18.1 Storage Format

- **Do not write tens of thousands of tiny JSONL files.** The v1 output of ~89K small files caused slow directory operations and I/O overhead. Consolidate into a small number of **Parquet files with zstd compression** (~1–5 GB each). This reads faster, compresses better, and is native to the HuggingFace `datasets` tooling used for training.
- **v3.0:** Parquet + zstd is the default for *all* intermediate stages, not just the final output. Sort within shards by `source_id` and `doc_id` for predictable scans, and keep a stable column order so schema evolution is visible in review.

### 18.2 Filesystem Placement

- Keep hot, in-process data on a **Linux-native filesystem (ext4/xfs)**, not a mounted NTFS drive. The observed ~2× slowdown came from WSL2 ↔ NTFS access over the 9P protocol.
- Raw data staged on HDD moves to the DGX Spark's NVMe for processing. Run heavy passes on **NVMe**, never against the HDD/NTFS mount.
- **v3.0:** maintain a **20–25% free-space reserve** on NVMe at all times, and never delete a stage's input until the output has passed validation, checksums, and manifest completion. See the companion DGX Spark guide for the full storage budget.

### 18.3 Hardware — Corrected Specification `CORRECTED in v3.0`

| Property | Value |
|----------|-------|
| CPU | **20-core Arm** (10× Cortex-X925 + 10× Cortex-A725) |
| Memory | **128 GB unified LPDDR5X**, shared between CPU and GPU |
| GPU | GB10 Grace Blackwell superchip |
| Storage | **4 TB NVMe** (this project's configuration) |
| Architecture | **aarch64** — not x86_64 |

Three consequences that invalidate v1/v2 planning assumptions:

1. **20 cores, not 72.** All CPU-stage throughput estimates and worker-count plans must be rebuilt around ~16 usable parallel workers.
2. **Unified memory is shared.** GPU allocations reduce what CPU stages can use; the 128 GB is not 128 GB of RAM *plus* VRAM.
3. **aarch64 wheels are not universal.** `kenlm`, `fasttext`, `sentencepiece`, `datasketch`, and PyArrow builds must be verified for arm64 at Stage −1, before the schedule depends on them.

### 18.4 Training Hardware — A Realistic Plan

- The **DGX Spark** is excellent for *building and running the pipeline*: encoding, dedup, the quality classifier, KenLM, tokenizer training, classification, evaluation, fine-tuning, and inference. Use it for everything *around* training.
- It is **not** a from-scratch pretraining rig for a 500M–1B model over tens of billions of tokens. **Rent a short H100/A100 multi-GPU run for the pretraining itself**, then bring the checkpoint back to the Spark for fine-tuning and serving.
- Budget the run explicitly with `FLOPs ≈ 6 × params × effective_tokens` (Section 17.1).

### 18.5 Evaluation Harness — Build It First

Wire up the evaluation harness (BLUB, BnSentMix, DIALTSA-BN, BanglaMath) **before** training begins so ablation variants can be compared. All held-out splits are reserved at Stage −1 (Section 4.3), not at Stage 9.

---

## 19. Reproducibility, Data Contracts & Stage Gates `NEW in v3.0`

### 19.1 Every Stage Is Deterministic and Resumable

Required properties for all stages:

- **immutable inputs** — a stage never modifies its input
- **atomic output publication** — write to a temp directory, then rename
- **shard-level retries** — one bad shard does not restart the stage
- **deterministic seeds**, recorded
- **config hash, code commit, model and tokenizer hashes**, recorded
- **reject and quarantine outputs** alongside accepted output
- **per-stage data contracts** — an explicit schema validated on read and write
- **manifest-based completion** — `_SUCCESS` written only after all validation passes

### 19.2 Standard Stage Layout

```text
stage_02_global_dedup/
  run_id=2026-02-01T09-00Z-a41f9c/
    config.yaml          # full resolved config
    manifest.json        # inputs, outputs, hashes, counts
    metrics.parquet      # per-shard and per-source metrics
    accepted/            # part-*.parquet (zstd)
    rejected/            # with reason codes
    quarantine/          # ambiguous — reviewable, recoverable
    samples/             # random audit samples for human review
    _SUCCESS
```

### 19.3 Universal Stage Gate

Every stage reports, and blocks on:

| Reported | Blocking condition |
|----------|--------------------|
| documents and tokens in/out | outside expected range for the source |
| byte volume in/out | expansion beyond benchmark prediction |
| counts by source / domain / script | a source silently vanishing |
| drop and quarantine reasons | one reason dominating unexpectedly |
| wall time and throughput | order-of-magnitude deviation from benchmark |
| peak CPU / GPU memory | approaching the unified-memory limit |
| config and code hashes | mismatch with the recorded run |
| random audit samples | failed spot check |
| regression vs. previous run | unexplained shift |

Use **token-weighted and document-weighted** metrics everywhere. A healthy document retention rate can hide catastrophic token loss (e.g. keeping every document but truncating each to a single paragraph).

---

## 20. Comparison with Published Approaches

| Dimension | TituLLMs (ACL 2025) | banglaLlama (LoResLM 2026) | FineWeb2 (2025) | SHUDDHIKARAN v3 |
|-----------|--------------------|-----------------------------|-----------------|-----------------|
| **Filtering granularity** | Document | Document | Language-adaptive | Chunk-level + document quality score |
| **Quality signal** | Heuristics | CulturaX filters | Model-based (edu classifier) | Model-based classifier (leakage-controlled) + KenLM feature |
| **LID threshold** | 95% | Not reported | Adaptive | Composite routing score, 70% doc / 85% chunk |
| **Deduplication** | Flat | Not reported | Per-language | Global early **external-memory** pass + validated per-domain chunk pass |
| **Backbone source** | CulturaX | CulturaX | Self (WARC) | FineWeb2 + HPLT + Sangraha (WARC) |
| **Code-mixed handling** | Dropped | Passive | N/A | Preserve-default, 5-way taxonomy, romanized rescue |
| **Dialect awareness** | None | None | None | 6-dialect detection with `unknown`, no `shuddho` default |
| **Tokenizer integration** | Post-hoc | Independent | N/A | Pipeline stage with candidate sweep and fertility gates |
| **Curriculum learning** | None | None | N/A | 4-tier, multi-epoch aware, **ablation-gated against shuffled baseline** |
| **Eval decontamination** | Not reported | Not reported | Not reported | Multi-signal, frozen at Stage −1, per-benchmark report |
| **Model sizing** | Fixed (1B, 3B) | Fixed (8B) | N/A | Data-constrained, over-trained, **pilot-validated epochs** |
| **Synthetic cap** | ~16% | N/A | N/A | ≤ 15% incl. source-synthetic, with collapse citation |
| **PII policy** | Not reported | Not reported | Not reported | Tiered contextual policy (public figures retained) |
| **Provenance** | Minimal | Minimal | Dataset-level | Per-document sidecar + dedup clusters + release class |
| **Reproducibility** | Not reported | Not reported | Partial | Config/code/model hashes, atomic stages, quarantine lane |

---

## 21. Novel Contributions

1. **Retention-first pipeline for low-resource languages** — every threshold justified against data-loss cost; scoring and second-chance routing over hard drops; a quarantine lane so "borderline" is a reviewable state rather than a deletion.
2. **Global-early external-memory deduplication** — a single cross-source exact + fuzzy pass, disk-backed and bounded-memory, before any expensive compute, yielding an auditable true unique-token count with full cluster provenance.
3. **Model-based quality scoring used as a ranker, not a filter** — FineWeb-Edu/DCLM-style learned quality with explicit source-leakage controls and subgroup false-positive reporting.
4. **Domain-conditional KenLM with dialect-aware fallback**, deployed as a *feature* by default and a gate only when register-calibrated.
5. **Dual-path code-mixed processing** with preserve-by-default, a five-way code-mixed taxonomy, and explicit rescue of romanized Bengali.
6. **Joint domain–dialect classification** — first Bengali pipeline to combine topical and regional labels, with an `unknown` class that keeps the audit honest.
7. **Morphology-aware tokenizer as a pipeline stage** with a candidate sweep, grapheme-fragmentation measurement, and pass/fail fertility gates.
8. **Bengali curriculum learning** with multi-epoch-aware tier repetition, benchmarked against a shuffled baseline instead of assumed.
9. **Data-constrained, over-trained model sizing** with a **pilot-validated** epoch multiplier.
10. **Per-stage retention + per-source dedup reporting**, token-weighted and document-weighted, with halt-on-anomaly.
11. **An openly-released, deduplicated, quality-scored Bengali corpus with per-document provenance and a licensing-aware dual release view.**
12. **Built-in ablation framework** — the first systematic data-quality study for Bengali.
13. **A reproducibility contract** `NEW` — atomic, resumable, hash-recorded stages with reject/quarantine lanes, making every published number regenerable.
14. **A contextual PII and safety policy for Bengali** `NEW` — tiered handling that preserves public-figure names, journalism, history, and counter-speech instead of redacting or deleting them.

---

## 22. Ablation Framework

Each variant produces a separate corpus with a unique version tag. Training identically on each and evaluating on standardized benchmarks (BLUB, BnSentMix, DIALTSA-BN, BanglaMath) produces the first systematic Bengali data-quality study.

| Variant ID | Modification | Research Question |
|-----------|-------------|-------------------|
| `abl-no-quality-classifier` | Skip Stage 3 | How much does the learned quality classifier help? |
| `abl-quality-hard-filter` | Stage 3 drops bottom 30% instead of ranking | Is ranking better than hard-dropping low-quality text? |
| `abl-aggressive-lid` | Stage 1 LID at 95% | How much valid data does aggressive LID discard? |
| `abl-multi-kenlm` | Use the 8-model KenLM fleet in Stage 5 | Does domain-conditional KenLM beat a single model? |
| `abl-kenlm-gate-on` | **(new)** KenLM as a hard gate vs. a ranker feature | Does perplexity gating help or just delete dialects? |
| `abl-no-second-chance` | Remove second-chance paths | Does second-chance rescue improve performance? |
| `abl-no-global-dedup` | Skip Stage 2 (chunk dedup only) | How critical is early global dedup? |
| `abl-dedup-perms` | **(new)** 128 vs. 256 MinHash permutations | Is 256 worth double the memory and I/O? |
| `abl-flat-chunk-dedup` | Stage 7 flat Jaccard 0.85 | Does domain-adaptive dedup outperform flat? |
| `abl-chunk-dedup-exact-only` | **(new)** Exact + boilerplate only, no fuzzy chunk dedup | Is fuzzy chunk dedup worth its cost and destruction? |
| `abl-rescue-off` | Stage 6 drops all code-mixed | Is code-mixed processing worth it? |
| `abl-rescue-restricted` | Stage 6 LLM-rescues only a high-value subset | Is restricted rescue better than none / than full? |
| `abl-preserve-all-codemixed` | Stage 6 preserves 100%, no translation | Is native preservation sufficient? |
| `abl-romanized-drop` | **(new)** Drop romanized Bengali instead of rescuing | Does the romanized register measurably help? |
| `abl-no-curriculum` | Random document ordering (**the v3 default arm**) | Does curriculum learning help Bengali? |
| `abl-reverse-curriculum` | Hard → easy ordering | Is easy-to-hard the correct direction? |
| `abl-competence-curriculum` | **(new)** Model-progress-paced curriculum | Does adaptive pacing beat fixed tiers? |
| `abl-no-dialect` | Drop all non-`shuddho` text | Does dialect exposure help or hurt standard tasks? |
| `abl-single-epoch` | Train 1 epoch vs. 4 | Does multi-epoch training help at this scale? |
| `abl-vanilla-tokenizer` | Vanilla SentencePiece | How much do morphology-aware tokenizer mods matter? |
| `abl-vocab-size` | **(new)** 32K / 48K / 64K | What vocabulary size is actually optimal for Bengali? |
| `abl-pii-aggressive` | **(new)** Redact all person names | What does blanket name redaction cost downstream? |

---

## 23. Execution Roadmap & Priority Order `EXPANDED in v3.0`

### 23.1 Phased Plan

**Phase A — Discovery & Governance (Stage −1)**
Source/license/checksum/eval registries; real schema inspection; stratified golden samples; annotation guidelines; arm64 dependency verification; 1% throughput benchmark.
*Exit gate:* every source readable, licensed, checksummed, benchmarked.

**Phase B — Pipeline Foundation**
Config-driven readers; versioned Parquet schemas; deterministic IDs and provenance; atomic writes, manifests, checkpoints, quarantine; unit, property, and golden-sample tests.
*Exit gate:* every reader passes schema and round-trip tests.

**Phase C — Baseline Corpus**
Encoding repair → conservative source-aware screening → exact dedup → external-memory fuzzy dedup → quality scoring → boundary-preserving chunking → exact/boilerplate chunk dedup → PII and safety policy → decontamination.
*Exit gate:* a usable corpus exists **without** domain classifiers, dialect classifiers, KenLM gating, LLM translation, or curriculum ordering.

**Phase D — Experimental Enhancements**
Each evaluated independently against Phase C: fuzzy chunk dedup, KenLM gating, learned quality ranker variants, domain classifier, dialect classifier, native code-mixed routing, synthetic translation rescue, curriculum ordering. Each must beat a predefined metric to enter production.

**Phase E — Tokenizer & Pilot Training**
Tokenizer candidate sweep → fertility and fragmentation evaluation → small pilot models on competing corpus variants → epoch-degradation curve → configuration selection from model outcomes.

**Phase F — Final Assembly & Pretraining Decision**
Freeze corpus version; generate provenance, dedup, and contamination reports; complete the human audit with agreement statistics; compute unique and effective token budgets; fit model size to secured compute; run scaling pilots before committing to the full run.

### 23.2 Recommended Order Within a ~3-Month Window

1. Stage −1 governance and benchmark (do not skip — it is the cheapest insurance in the project).
2. Download backbone: **FineWeb2 (bn) + Sangraha**; also pull HPLT buckets 1–4.
3. Normalize all sources to the unified schema (Stage 0) and run Stage 1.
4. Run the **global dedup pass (Stage 2)** — this yields the true token count and tells you whether the target model size is achievable at all.
5. Train and apply the **quality classifier (Stage 3)**.
6. Chunking, exact/boilerplate chunk dedup, QC, decontamination.
7. Tokenizer sweep + assembly.
8. Small **proof-of-concept training run** to prove the data works, then scale on rented GPUs.
9. Add LLM code-mixed rescue, the multi-KenLM fleet, dialect/domain classifiers, and curriculum ordering **only if** ablations show they help and time remains.

### 23.3 Priority Classification

| Must have | Should have | Research extension |
|-----------|-------------|--------------------|
| Governance & source registry | Learned quality ranking refinements | Dialect classifier |
| Reproducible ingestion | Native code-mixed preservation | Domain-specific KenLM fleet |
| Exact dedup | Contextual PII handling | LLM translation rescue |
| Scalable external-memory fuzzy dedup | Boilerplate detection | Strict curriculum ordering |
| Provenance preservation | Source/domain mixing weights | Morphology-prioritized tokenizer training |
| Conservative quality processing | | Domain-specific fuzzy thresholds |
| Decontamination | | |
| Tokenizer experiments | | |
| Pilot-model validation | | |

---

## 24. References

1. Bhattacharjee, A., et al. (2022). BanglaBERT: Language Model Pretraining and Benchmarks for Low-Resource Language Understanding Evaluation in Bangla. *Findings of NAACL*. arXiv:2203.14840.
2. Nahin, A. F., et al. (2025). TituLLMs: Building and Evaluating LLMs for Bangla. *Proceedings of ACL 2025*. arXiv:2502.11187.
3. banglaLlama (2026). A Family of Open-Source Instruction-Tuned Bengali Language Models. *Proceedings of LoResLM 2026 (ACL Workshop)*.
4. BengaliBPE (2025). Morphology-Aware Byte-Pair Encoding for Bengali Script. *ResearchGate preprint*.
5. IndicSuperTokenizer (2025). Language-Specific Pre-Tokenization for Indic Multilingual LLMs. *arXiv preprint*.
6. DIALTSA-BN (2025). Dialect-to-Standard Translation and Sentiment Analysis for Bengali. *Proceedings of BLP 2025 (EMNLP Workshop)*.
7. BnSentMix (2025). A Benchmark Dataset for Bengali-English Code-Mixed Sentiment Analysis. *Proceedings of BLP 2025*.
8. Alam, K. S. Y. (2025). MixSarc: A Bangla-English Code-Mixed Corpus for Sarcasm Detection. *arXiv preprint*.
9. Penedo, G., et al. (2024). FineWeb: Decanting the Web for the Finest Text Data at Scale. *NeurIPS 2024 Datasets & Benchmarks*.
10. Penedo, G., et al. (2025). FineWeb2: A Massive Multilingual Text Dataset for LLM Pretraining. *arXiv preprint*.
11. Kudugunta, S., et al. (2023). MADLAD-400: A Multilingual and Document-Level Large Audited Dataset. *NeurIPS 2023*.
12. de Gibert, O., et al. (2024). A New Massive Multilingual Dataset for High-Performance Language Technologies (HPLT). *arXiv preprint*.
13. AI4Bharat (2024). Sangraha / IndicLLMSuite: A Blueprint for Pretraining Data for Indian Languages. *arXiv preprint*.
14. Li, R., et al. (2024). DataComp-LM (DCLM): In Search of the Next Data Frontier for Language Models. *NeurIPS 2024*.
15. Magnusson, I., et al. (2025). REWIRE: Recycling the Web for Better Language Model Pretraining. *arXiv preprint*.
16. Muennighoff, N., et al. (2023). Scaling Data-Constrained Language Models. *NeurIPS 2023*. (Multi-epoch data repetition.)
17. Hoffmann, J., et al. (2022). Training Compute-Optimal Large Language Models. *NeurIPS 2022*. (Chinchilla scaling laws.)
18. Shumailov, I., et al. (2024). AI Models Collapse When Trained on Recursively Generated Data. *Nature*, 631, 755–760.
19. ACL 2026. Systematic Investigation of Curriculum Learning for Large Language Model Pretraining. *Proceedings of ACL 2026*.
20. DUCL (2026). Difficulty Is Not Enough: Utility-Driven Curriculum Learning for LLMs. *Proceedings of AAAI 2026*.
21. Broder, A. Z. (1997). On the Resemblance and Containment of Documents. *Compression and Complexity of Sequences*.
22. Heafield, K. (2011). KenLM: Faster and Smaller Language Model Queries. *Proceedings of WMT 2011*.
23. Brown, T. B., et al. (2020). Language Models are Few-Shot Learners. *NeurIPS 2020*. (GPT-3 decontamination methodology.)
24. Penedo, G., et al. (2023). The RefinedWeb Dataset for Falcon LLM. *NeurIPS 2023*.
25. Lee, K., et al. (2022). Deduplicating Training Data Makes Language Models Better. *ACL 2022*.
26. Elazar, Y., et al. (2024). What's In My Big Data? *ICLR 2024*. (Corpus auditing and contamination analysis.)
27. Gebru, T., et al. (2021). Datasheets for Datasets. *Communications of the ACM*, 64(12).
28. Mitchell, M., et al. (2019). Model Cards for Model Reporting. *FAccT 2019*.

---

*SHUDDHIKARAN Pipeline — Version 3.0 (Final). v2.0 architecture preserved; execution, reproducibility, governance, and hardware assumptions hardened.*
