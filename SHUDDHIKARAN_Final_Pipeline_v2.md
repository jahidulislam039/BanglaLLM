# SHUDDHIKARAN: A Retention-First Pre-Training Data Pipeline for Bengali Language Models

**Version 2.0 — Final**

---

## Abstract

This document specifies **SHUDDHIKARAN** (শুদ্ধিকরণ — *purification*), a pre-training data pipeline engineered for the construction of a Bengali-native large language model (LLM). The pipeline processes raw Bengali text from a large set of heterogeneous sources and is designed around a central thesis: **current data processing methodologies for low-resource languages are excessively wasteful.** Filtering strategies developed for English-scale data abundance (15T+ tokens) are routinely applied to Bengali corpora, where the marginal cost of discarding a valid document is permanent and irrecoverable information loss.

SHUDDHIKARAN inverts this default. The pipeline is architected to *rescue first and drop last*, deploying multi-path routing, second-chance validation, and domain-conditional quality gates to maximize the volume of clean, high-quality Bengali text surviving into the final training corpus.

Version 2.0 revises the original design in five important ways, based on a review of what actually maximizes the value of a foundational Bengali model:

1. **A curated, WARC-extracted backbone.** FineWeb2, HPLT v2, and AI4Bharat Sangraha are adopted as the primary high-quality core of the corpus. The project's own WET-extracted CommonCrawl crawls become a supplementary recall source rather than the backbone.
2. **Deduplication is moved early and made global.** A single global exact + fuzzy dedup pass runs across *all* sources immediately after ingestion, before any expensive per-chunk or LLM processing. This prevents wasted compute on documents that are about to be removed.
3. **A model-based quality classifier is added.** A lightweight fastText classifier scores every document for quality and is used to *rank* (not hard-drop) text, consistent with the retention-first philosophy. This is the single highest-impact modern quality technique the original pipeline was missing.
4. **The model size is decoupled from the Chinchilla ratio.** Because the goal is the largest useful foundational model, the corpus is planned for multi-epoch (data-constrained) training rather than a single Chinchilla-optimal pass.
5. **Explicit engineering and infrastructure guidance.** Storage format, filesystem placement, and a realistic training-hardware plan (DGX Spark for everything *around* training; rented multi-GPU for the pretraining run itself) are now first-class parts of the specification.

The pipeline retains its original methodological contributions — chunk-level rescue, domain-conditional perplexity gating with dialect-aware fallback, dual-path code-mixed processing, morphology-aware tokenizer co-design, curriculum learning, and a built-in ablation framework.

---

## Table of Contents

1. [Project Objective & Design Philosophy](#1-project-objective--design-philosophy)
2. [Raw Data Inventory](#2-raw-data-inventory)
3. [Pipeline Architecture](#3-pipeline-architecture)
4. [Stage 0 — Encoding Foundation](#4-stage-0--encoding-foundation)
5. [Stage 1 — Document-Level Pre-screening](#5-stage-1--document-level-pre-screening)
6. [Stage 2 — Global Deduplication (Early Pass)](#6-stage-2--global-deduplication-early-pass)
7. [Stage 3 — Model-Based Quality Scoring](#7-stage-3--model-based-quality-scoring)
8. [Stage 4 — Domain & Dialect Classification](#8-stage-4--domain--dialect-classification)
9. [Stage 5 — Sub-Document Processing](#9-stage-5--sub-document-processing)
10. [Stage 6 — Dual-Path Code-Mixed Processing](#10-stage-6--dual-path-code-mixed-processing)
11. [Stage 7 — Chunk-Level Deduplication](#11-stage-7--chunk-level-deduplication)
12. [Stage 8 — Final Quality Control](#12-stage-8--final-quality-control)
13. [Stage 9 — Evaluation Set Decontamination](#13-stage-9--evaluation-set-decontamination)
14. [Stage 10 — Tokenizer Co-Design & Fertility Validation](#14-stage-10--tokenizer-co-design--fertility-validation)
15. [Stage 11 — Curriculum Scoring & Training Manifest](#15-stage-11--curriculum-scoring--training-manifest)
16. [Stage 12 — Corpus Assembly, Model Sizing & Audit](#16-stage-12--corpus-assembly-model-sizing--audit)
17. [Engineering & Infrastructure](#17-engineering--infrastructure)
18. [Comparison with Published Approaches](#18-comparison-with-published-approaches)
19. [Novel Contributions](#19-novel-contributions)
20. [Ablation Framework](#20-ablation-framework)
21. [References](#21-references)

---

## 1. Project Objective & Design Philosophy

### 1.1 Objective

Ground-up pre-training of a generative Bengali foundation model, sized to be **as large as the available data and compute reasonably allow**. The model is trained exclusively on data processed through this pipeline.

### 1.2 Model Sizing — Beyond the Chinchilla Cap

The original specification fixed the model size at the Chinchilla-optimal ratio:

```
model_parameters = surviving_clean_tokens / 20
```

**Version 2.0 abandons this as a hard cap, for a specific reason.** Chinchilla's 20:1 ratio (Hoffmann et al., 2022) is *compute-optimal for a fixed one-time training budget* — it minimizes loss per FLOP. It is **not** how modern foundational models are built. Llama 2/3, Qwen, and Gemma are all deliberately *over-trained*, using 100–200+ tokens per parameter, because training compute is paid once but inference savings last forever.

Two consequences for this project:

1. **The real data ceiling is not "unique tokens ÷ 20."** Under data-constrained scaling (Muennighoff et al., 2023), pretraining data can be repeated for up to roughly **4 epochs with almost no loss penalty**. So the effective training budget is approximately `unique_tokens × 4`, not `unique_tokens`. A corpus of ~15–18B unique tokens therefore supports on the order of **60–70B effective training tokens**.

2. **The most important lever for a "big" model is more *unique* data**, not the sizing formula. This is why Version 2.0 adds the FineWeb2 / HPLT / Sangraha backbone (Section 2) and a global dedup pass (Section 6) — together they determine the true unique-token count that everything else follows from.

The final model size is therefore chosen at Stage 12 as a **joint function of unique tokens, the achievable epoch count, and the training compute actually secured** — not from a single fixed ratio. See Section 16 for the sizing table.

### 1.3 Design Philosophy

Three principles govern every design decision in this pipeline:

**Principle 1 — Retention over aggression.** For a language with finite available data, the cost of rescuing a marginal document is a few CPU cycles; the cost of dropping a valid document is permanent information loss. Every filtering threshold must justify the data it discards, and every drop decision must be compensated by a rescue path where feasible. Wherever possible the pipeline *scores and ranks* rather than hard-drops.

**Principle 2 — Quality at the right granularity.** Document-level filtering is appropriate for English, where 15 trillion tokens of replacement data exist. For Bengali, quality control operates at *chunk granularity* — a document containing 40% noise and 60% valid Bengali still contributes 60% of irreplaceable text when processed at the sub-document level.

**Principle 3 — Data engineering, not just data cleaning.** The pipeline's outputs include not only clean text, but also quality scores, domain labels, dialect annotations, tokenizer fertility measurements, curriculum difficulty scores, and a training-order manifest. Data composition, ordering, and tokenizer co-design are first-class pipeline products.

**Principle 4 (new) — Spend compute only on data that will survive.** Expensive operations (LLM translation, per-chunk perplexity, tokenization) must run *after* deduplication and cheap filtering, never before. Every unit of GPU time spent on a document that is later removed is waste.

### 1.4 Conventions

Throughout this document, the following markers indicate the provenance and novelty of design decisions relative to the published literature:

| Marker | Meaning |
|--------|---------|
| `SOTA` | Matches current best practice from published work (FineWeb2, DCLM, REWIRE, etc.) |
| `BEYOND-SOTA` | Exceeds what any published Bengali or multilingual pipeline has demonstrated |
| `NOVEL` | Original contribution with no published precedent in Bengali NLP |

---

## 2. Raw Data Inventory

The corpus is organized into two tiers: a **curated backbone** of pre-cleaned, WARC-extracted sources that form the quality core, and a **supplementary tier** of the project's own crawls and specialist collections that add volume, register coverage, and rare domains.

### 2.1 Tier 1 — Curated Backbone (WARC-extracted, pre-filtered)

These sources are already extracted from raw HTML (WARC) and SOTA-filtered by their publishers. They require no WARC re-extraction on our part and form the high-quality core of the corpus.

| Source | Nature | Why it matters |
|--------|--------|----------------|
| **FineWeb2 (`bn_Beng`)** | WARC-extracted, deduplicated, quality-filtered web text | Highest-quality open web Bengali in existence. Primary backbone. |
| **HPLT v2 (`ben_Beng`, sorted)** | Cleaned, document-level, quality/register-sorted shards | Large clean web corpus with per-document quality metadata. See Section 2.4. |
| **AI4Bharat Sangraha (Bengali)** | Verified + synthetic, includes romanized Bengali | Adds the romanized/"Banglish" register that is otherwise almost absent. |
| **MADLAD-400 (`bn`)** | Cleaned multilingual CC derivative | Extra coverage and a cross-dedup reference. |

> **On WARC vs. WET.** The original pipeline extracted CommonCrawl text from **WET** files (CommonCrawl's naive text dump, which retains navigation, menus, and boilerplate). Re-extracting from **WARC** with Trafilatura/resiliparse produces far cleaner documents and is what FineWeb2 does. **This project does not re-extract from WARC**, for a deliberate reason: the Tier 1 backbone above is *already* WARC-extracted and SOTA-filtered, so the high-quality core is covered without spending the time. The project's existing WET crawls are retained as a supplementary recall source (Tier 2), where WET-grade cleanliness plus the quality classifier (Stage 3) is adequate.

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

---

## 3. Pipeline Architecture

The stage order is revised in Version 2.0 so that **cheap filtering and global deduplication happen before any expensive per-chunk or LLM work.**

```
Tier 1 backbone (FineWeb2, HPLT, Sangraha, MADLAD)  +  Tier 2 supplementary (WET, mC4, news, books, ...)
    │
    ▼
Stage 0: Encoding Foundation
    │    Bijoy→Unicode, NFC normalization, ZWNJ canonicalization, PUA stripping
    │    Retention: ~100% (repair, not removal)
    ▼
Stage 1: Document-Level Pre-screening
    │    FastText LID, length filters, structural noise ratios, coarse PII scan
    │    Retention target: >=90%
    ▼
Stage 2: GLOBAL Deduplication (early)            <-- moved up, now global
    │    Exact (SHA-256) + fuzzy (MinHash LSH) ACROSS ALL SOURCES
    │    Emits per-source dedup stats -> true unique-token count
    │    Retention: ~60-80% of documents
    ▼
Stage 3: Model-Based Quality Scoring             <-- NEW
    │    fastText quality classifier scores every document (rank, don't drop)
    │    Retention: ~100% (scoring only; low scores kept but down-weighted)
    ▼
Stage 4: Domain & Dialect Classification
    │    10-domain taxonomy, 6-dialect detection
    │    Retention: ~100% (labelling only)
    │
    ├──────────────────────────────────────────────┐
    ▼                                              ▼
Stage 5a: Clean Bengali Chunks                 Stage 5b: Code-Mixed Chunks
    │    Domain-conditional KenLM                   │    Bengali script ratio 0.25-0.80
    │    with second-chance routing                  ▼
    │                                        Stage 6: Dual-Path Code-Mixed Processing
    │                                          (LLM rescue is optional/restricted - see 10.7)
    └───────────────────────────────┬───────────────┘
                                     ▼
                        Stage 7: Chunk-Level Deduplication
                                     ▼
                        Stage 8: Final Quality Control
                                     ▼
                        Stage 9: Evaluation Set Decontamination
                                     ▼
                        Stage 10: Tokenizer Co-Design & Fertility Validation
                                     ▼
                        Stage 11: Curriculum Scoring & Training Manifest
                                     ▼
                        Stage 12: Corpus Assembly, Model Sizing & Audit
                                     ▼
                        Model Architecture Specification + Training Plan
```

**Key ordering changes from v1:**

- **Global deduplication (Stage 2) now runs early**, before quality scoring, chunking, KenLM, and any LLM translation. In v1, dedup happened at Stage 5 — *after* Qwen-72B had already translated code-mixed chunks that were about to be deleted. That is now impossible.
- **A model-based quality classifier (Stage 3) is inserted** right after dedup so that every later stage can use the quality score.
- **Chunk-level dedup (Stage 7)** is separated from global document dedup, since it targets a different granularity and runs after chunking.

---

## 4. Stage 0 — Encoding Foundation

**Purpose:** Repair encoding-level corruption before any downstream processing. Encoding errors propagate silently and corrupt all filtering, hashing, deduplication, and tokenizer training. This stage converts and normalizes — it does not discard documents.

**Retention impact:** ~100%.

### 4.1 Bijoy Encoding Detection & Conversion

Legacy Bangladeshi legal, institutional, and journalistic text was produced in Bijoy encoding — a proprietary non-Unicode standard still common in government archives and court records. Bijoy-encoded bytes passed through a UTF-8 pipeline do not raise exceptions; they silently produce garbage codepoints that survive all language filters.

- Detect Bijoy-encoded files via byte-range heuristics (>70% of bytes in the 0x80–0xFF range with characteristic Bijoy byte signatures).
- Apply a validated Bijoy→Unicode codepoint mapping table.
- Validate conversion quality against a manually verified 1% sample before processing at scale.
- Tag all converted documents with `"encoding": "bijoy_converted"` in the provenance record.

> The Legal Documents source (876 `.txt` files) is the highest-probability Bijoy source and should be processed first as a calibration exercise. Note that Tier 1 backbone sources (FineWeb2, HPLT, Sangraha) are already clean Unicode and can skip Bijoy detection.

### 4.2 Unicode NFC Normalization

Bengali has multiple valid Unicode representations for the same visible glyph due to different orderings of vowel matras, hasanta (virama), and nukta. These representations are semantically identical but lexicographically different.

```python
import unicodedata
text = unicodedata.normalize('NFC', text)
```

Applied to every document and every chunk before any string comparison, hash computation, or language model scoring. Without NFC normalization, MinHash LSH treats identical texts as different (causing deduplication misses) and BPE tokenizers learn spurious sub-word units for the same grapheme cluster. **This is a prerequisite for the global dedup pass in Stage 2 to work correctly across sources.**

### 4.3 ZWNJ Canonicalization

Bengali has linguistically valid uses of the zero-width non-joiner (ZWNJ, U+200C) to force explicit *juktakkhor* (compound character) decomposition. Web-scraped text contains many spurious ZWNJs inserted by font-rendering workarounds.

```python
import re
# Preserve ZWNJ only between virama (U+09CD) and a following consonant
text = re.sub(r'(?<!\u09CD)\u200C', '', text)
```

### 4.4 Private Use Area & Variation Selector Stripping

Strip all Unicode Private Use Area codepoints (U+E000–U+F8FF) and variation selectors (U+FE00–U+FE0F) originating from proprietary font conversions. These carry no semantic content and pollute tokenizer vocabulary.

```python
text = re.sub(r'[\uE000-\uF8FF\uFE00-\uFE0F]', '', text)
```

### 4.5 Source-Specific Format Extraction

Each source requires format-specific extraction before entering the unified pipeline. All sources are normalized to a **single unified schema** (`{doc_id, text, source_dataset, source_url, license, ...}`) so that the global dedup pass can operate across them.

| Source | Extraction Strategy |
|--------|-------------------|
| FineWeb2 / HPLT / MADLAD (`.jsonl.zst` / Parquet) | Read `text` field; preserve publisher quality metadata |
| Sangraha | Read text; tag romanized vs. native script |
| mC4 (`.txt` parts) | Split on double newlines |
| Newspaper JSON (ebD) | Parse JSON; extract article body; preserve metadata |
| IndicCorpV2 / CC-100 (monolith `.txt`) | Reconstruct documents from blank-line boundaries |
| Bengali Wikipedia (`.arrow`) | Read via PyArrow; extract `text` column |
| OCR'd NCTB / Legal (`.txt`) | Direct ingestion (Legal: Bijoy priority) |
| Banglapedia | Per-article files; one file = one document |
| Samanantar | Extract Bengali side only |

---

## 5. Stage 1 — Document-Level Pre-screening

**Purpose:** A fast, inexpensive coarse pass eliminating obviously unusable documents before deduplication and expensive per-chunk processing. Thresholds are deliberately conservative — borderline documents are passed to later chunk-level rescue.

**Retention target:** ≥90%.

### 5.1 FastText Language Identification

Run Meta's FastText LID model (`lid.176.bin`). Threshold: **>70% Bengali confidence**.

This threshold is intentionally lower than the 95% used by TituLLMs (Nahin et al., 2025). At 95%, the pipeline loses most code-mixed social media content, blog posts with English technical terms, and bilingual educational materials. The real quality gate operates at chunk granularity in Stage 5 (>85% Bengali confidence on the clean path), making aggressive document-level filtering unnecessary and wasteful.

> Tier 1 backbone sources already carry publisher language scores; their LID can be trusted and this step used only as a light sanity check.

### 5.2 Document Length Filter

| Condition | Action | Rationale |
|-----------|--------|-----------|
| < 50 tokens | Drop | Insufficient content for any training signal |
| > 200,000 tokens | Drop | Likely a concatenation artifact |
| 50–200,000 tokens | Pass | Legal clauses and poem stanzas are often 50–100 tokens |

### 5.3 Structural Noise Ratio Filter

| Signal | Threshold | Rationale |
|--------|-----------|-----------|
| URL density (URLs / total tokens) | > 20% | Spam and link-farm content |
| Punctuation + symbol density | > 35% | Malformed OCR or encoding artifacts |
| Number density | > 50% | Tables scraped as text |
| Repeated line ratio (line seen > 3×) | > 30% | Boilerplate and navigation menus |
| Average line length | < 3 tokens | Navigation fragments (poetry preserved) |

### 5.4 Coarse PII Scan

Fast regex scan for Bangladesh-specific personally identifiable information:

- Bangladesh NID number patterns: `\d{10}`, `\d{13}`, `\d{17}`
- Phone numbers: `(\+880|880|0)[- ]?\d{9,11}`
- Email addresses: standard RFC 5322 regex
- Bank account numbers: common Bangladeshi bank formats

Documents exceeding 5 PII instances per 1,000 tokens are flagged for redaction at Stage 8 — not dropped. PII is a redaction problem, not a disqualification event.

### 5.5 Retention Anomaly Gate

At Stage 1 completion, compute the retention rate. If it falls below 85%, the pipeline halts and requires a threshold audit before proceeding. Sub-85% retention at the coarse screening stage indicates a systemic problem with the source data or the thresholds, not with individual documents.

---

## 6. Stage 2 — Global Deduplication (Early Pass) `BEYOND-SOTA`

**Purpose:** Remove duplicate content across *all* sources, once, before any expensive processing. This is now the pipeline's most important structural stage: it converts a pile of overlapping corpora into a real, countable set of unique documents, and it prevents downstream compute from being spent on data that will be discarded.

**Why this moved from Stage 5 to Stage 2.** In v1, deduplication happened *after* per-chunk KenLM scoring and — critically — *after* Qwen-72B translation of code-mixed chunks. That meant the pipeline could spend 72B-parameter model inference translating duplicated Banglish that was then deleted. Running dedup early eliminates this waste entirely. FineWeb also showed that dedup ordering materially affects final quality, not just cost.

**Why this is global, not per-file.** The v1 extractor hashed only *within a shard*. But the biggest source of duplication is *cross-source* (FineWeb2 ↔ HPLT ↔ mC4 ↔ CC-100 ↔ own WET all overlap heavily). Dedup must therefore be a single pass over the entire merged, unified-schema corpus.

**Retention:** ~60–80% of documents (source-dependent).

### 6.1 Exact Deduplication

Compute the **SHA-256 hash** of each Unicode-normalized, whitespace-collapsed document. Group by hash; retain one instance of each exact duplicate. This O(n) pass eliminates verbatim copies — common in news wire syndication, legal boilerplate, and CC re-crawls — with zero tuning.

### 6.2 Fuzzy Deduplication (Global MinHash LSH)

`datasketch` MinHash LSH with 256 permutations at a global **Jaccard threshold of 0.85** across the entire corpus. This removes near-duplicates (same article with minor edits, CMS boilerplate, re-crawled pages with changed timestamps).

Domain-calibrated thresholds are *not* applied at this global stage — domain labels do not yet exist (they are assigned in Stage 4). Domain-aware and chunk-level dedup happen later at Stage 7. The global pass uses a single conservative threshold to catch the bulk of cross-source duplication safely.

### 6.3 Source-Priority Retention

When duplicates are found across sources, keep the copy from the **highest-quality source** so the surviving document carries the best metadata:

```
priority: FineWeb2 > HPLT > Sangraha > Wikipedia/Banglapedia > curated news
          > IndicCorpV2 > MADLAD > CC-100 > mC4 > own WET crawls
```

### 6.4 Dedup Statistics Report (required output)

The global dedup pass must emit a per-source report. This is what tells us the *true* size of the corpus and which sources are actually adding value.

```json
{
  "stage": 2,
  "documents_in": 0,
  "documents_out": 0,
  "per_source": {
    "fineweb2":   {"in": 0, "unique_kept": 0, "removed_as_dup": 0, "net_new_pct": 0.0},
    "hplt_v2":    {"in": 0, "unique_kept": 0, "removed_as_dup": 0, "net_new_pct": 0.0},
    "own_wet_cc": {"in": 0, "unique_kept": 0, "removed_as_dup": 0, "net_new_pct": 0.0}
  },
  "estimated_unique_tokens": 0
}
```

> **Expectation:** the own WET crawls, mC4, and CC-100 will show low `net_new_pct` after deduping against FineWeb2/HPLT (they are largely the same underlying CC pages). This is expected and fine — they remain useful as recall and for source-priority fallbacks.

---

## 7. Stage 3 — Model-Based Quality Scoring `SOTA` `NEW`

**Purpose:** Score every surviving document with a lightweight learned quality classifier. This is the single most impactful modern data-quality technique (used by FineWeb-Edu and DCLM) and was absent from v1, which relied entirely on KenLM perplexity plus hand-tuned density heuristics.

**Philosophy alignment:** the classifier is used as a **scorer and ranker, not a hard filter.** Every document keeps its score; low-scoring documents are down-weighted in the mixing ratios (Stage 12) and sorted into harder curriculum tiers (Stage 11), but are not automatically discarded. This preserves the retention-first principle while still letting the model learn preferentially from high-quality text.

**Retention:** ~100% (scoring only).

### 7.1 Classifier Design

- **Model:** fastText supervised classifier (fast enough to score the entire corpus on CPU), or a small linear head on multilingual embeddings if higher accuracy is needed.
- **Positive examples (high quality):** Bengali Wikipedia, Banglapedia, NCTB textbooks, curated post-2015 news (Prothom Alo, bdnews24), Bengali literature, FineWeb2's highest-quality bucket.
- **Negative examples (low quality):** raw low-LID web crawl, SEO/spam pages, boilerplate-heavy WET documents, machine-garbled text.
- **Output:** a continuous `quality_score` in [0, 1] per document.

### 7.2 How the Score Is Used Downstream

| Consumer | Use of `quality_score` |
|----------|------------------------|
| Stage 11 (curriculum) | Low score → later/harder tier; high score → foundation tier |
| Stage 12 (mixing) | Mixing weights lean toward high-score documents |
| Own WET crawls | Expected to score low on average; kept but down-weighted, so the backbone dominates |
| Ablations | `abl-no-quality-classifier` toggles this stage off (Section 20) |

### 7.3 Why This Replaces Most of the KenLM Fleet's Burden

The v1 design used **8 domain-specific KenLM models** plus a BanglaBERT domain classifier as the primary quality mechanism. That is a lot of coupled components where a single misclassification cascades into false drops. In v2:

- The **learned quality classifier** becomes the primary quality signal.
- **KenLM is retained** for perplexity-based *chunk* scoring (Stage 5) and curriculum difficulty (Stage 11), but the full 8-model fleet is **optional and ablation-gated** (Section 20). A single robust KenLM trained on Wikipedia + books + curated news is the default; the fleet must earn its complexity in an ablation before being adopted.

---

## 8. Stage 4 — Domain & Dialect Classification `NOVEL`

**Purpose:** Assign a domain label and a dialect label to every document. These labels govern downstream behavior: KenLM model selection (Stage 5), chunk-level deduplication thresholds (Stage 7), curriculum difficulty scoring (Stage 11), and final mixing ratios (Stage 12).

**Retention impact:** ~100%. This stage classifies — it does not filter.

### 8.1 Domain Classifier

Architecture: **BanglaBERT-based multi-class classifier**, fine-tuned on ~10,000 human-labelled documents across 10 domains.

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

**Confidence gate:** If softmax maximum < 0.6, assign `web_general` as the default label. All low-confidence classifications are logged.

### 8.2 Dialect Detection Layer `NOVEL`

A secondary **BanglaBERT-based dialect classifier** (~5,000 labelled samples) detects regional variation.

| Dialect ID | Region | Approximate L1 Speakers |
|------------|--------|------------------------|
| `shuddho` | Standard Bengali (Kolkata/Dhaka norm) | ~170M |
| `chattogram` | Chittagong Division | ~16M |
| `sylhet` | Sylhet Division | ~11M |
| `barishal` | Barisal Division | ~8M |
| `rangpur` | Rangpur Division | ~15M |
| `noakhali` | Noakhali / Comilla region | ~9M |

**Classification logic:**
- Dialect confidence > 0.70 for any non-`shuddho` category → assign that dialect label.
- Confidence < 0.70 for all non-standard categories → assign `shuddho` (default).
- Both domain and dialect labels are written to the document provenance record.

**Rationale:** Without dialect detection, dialectal text produces anomalously high perplexity against a standard-Bengali KenLM and is dropped as noise. Dialect detection is a prerequisite for retaining this data and for training a model that serves all 230M+ Bengali speakers.

---

## 9. Stage 5 — Sub-Document Processing `BEYOND-SOTA`

**Purpose:** The core data rescue mechanism. Documents are split into chunks, and each chunk is independently scored, routed, and either accepted, rescued, or dropped. A document containing 40% noise and 60% valid Bengali contributes its 60% — the noise is removed, not the document.

**Retention target:** ≥85% of chunks.

### 9.1 Semantic Chunking

Split each document into chunks using the following priority order:

1. **Paragraph breaks** (`\n\n`): primary split boundary.
2. **Sentence boundary** (Bengali sentence tokenizer): secondary split when a paragraph exceeds 512 tokens.
3. **Hard token limit** at 512 tokens: absolute maximum using hard breaks. No sliding window — avoids double-counting.

Post-chunking rules:
- Discard chunks below **20 tokens** — accommodates Bengali poetry couplets and legal sub-clauses.
- Tag each chunk with: source document ID, position index, domain label, dialect label, document quality score.

### 9.2 Per-Chunk Script Analysis & Routing

For each chunk, compute the Bengali script ratio:

```python
def bengali_ratio(text):
    bengali = sum(1 for c in text if '\u0980' <= c <= '\u09FF')
    non_space = sum(1 for c in text if not c.isspace())
    return bengali / non_space if non_space > 0 else 0
```

| Bengali Script Ratio | Route |
|----------------------|-------|
| ≥ 0.80 | **CLEAN** → Stage 5.3 (KenLM quality gate) |
| 0.25 – 0.80 | **CODE-MIXED** → Stage 6 (dual-path processing) |
| < 0.25 | **DROP** — predominantly non-Bengali |

Additionally, run per-chunk FastText LID on the clean path with a **>85% Bengali confidence** threshold.

### 9.3 KenLM Perplexity Gating (simplified default)

**Default (v2):** a single robust 5-gram KenLM trained on Wikipedia + Bengali books + curated post-2015 news is used for perplexity scoring of clean chunks.

**Optional (ablation-gated):** the original fleet of domain-specific KenLM models (`kenlm_news`, `kenlm_legal`, `kenlm_educational`, `kenlm_literature`, `kenlm_encyclopedic`, `kenlm_web`, `kenlm_medical`, `kenlm_social`) may be used instead, selected by the Stage 4 domain label. This is retained as ablation variant `abl-multi-kenlm` and adopted only if it demonstrably improves downstream quality.

**Gating logic with second-chance routing:**

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
      -> DROP - genuinely unrecoverable noise.

  IF perplexity < P3:
      -> FLAG for manual review (possible boilerplate).
```

**Dialect exception:** Chunks tagged with any non-`shuddho` dialect label are always scored against the most permissive (web) KenLM. Dialectal Bengali scores anomalously high against standard-register models; permissive routing prevents false drops.

### 9.4 Document Survival Decision

After chunk-level filtering, compute the chunk survival ratio per source document:

```
survival_ratio = surviving_chunks / total_chunks
```

| Survival Ratio | Action |
|----------------|--------|
| ≥ 0.50 | Reconstruct from surviving chunks |
| 0.20 – 0.50 | Reconstruct if surviving token count ≥ 150 tokens |
| < 0.20 | Drop entire document |

Insert gap markers `[...]` between non-adjacent surviving chunks to prevent false sentence continuity across dropped sections.

---

## 10. Stage 6 — Dual-Path Code-Mixed Processing `NOVEL`

**Purpose:** Handle chunks containing a mixture of Bengali and English text (Bengali script ratio 0.25–0.80).

**Rationale:** Real Bengali internet users write extensively in code-mixed "Banglish." The BnSentMix dataset (2025) and MixSarc corpus (2026) demonstrate that code-mixed text is a valuable training signal. A model trained exclusively on monolingual Bengali will fail on a significant fraction of real-world input.

### 10.1 Important Revision — LLM Translation Is Now Optional and Restricted

The v1 design routed 70% of code-mixed chunks through **Qwen-72B translation**. In v2 this is reconsidered for two reasons: (a) translating millions of chunks with a 72B model is an enormous compute cost, and (b) large-scale synthetic rescue carries real model-collapse / distribution-shift risk (Shumailov et al., 2024).

**Default (v2):** **Path B — Native Preservation** for all viable code-mixed chunks. Code-mixed text is preserved in its natural form (it is a real, useful register), subject to the quality gates in Section 10.4.

**Optional (ablation-gated):** **Path A — LLM Rescue** is applied only to a small, high-value subset (e.g. code-mixed chunks in the `news`/`educational` domains with Bengali ratio 0.50–0.80), and may use a smaller/faster translation model rather than 72B. This is controlled by ablation variants `abl-rescue-restricted` and `abl-rescue-off`.

Because dedup now runs at Stage 2, whatever translation is performed is never spent on duplicated chunks.

### 10.2 Path A — Translation (when enabled)

**Pre-screening:** Remove only pure noise before translation (stop-word ratio > 90%, emoji/hashtag-only content, keyword lists without sentence structure).

**Translation engine:** a locally-served instruction model (Qwen-family, Apache 2.0) via vLLM with continuous batching. Prefer a smaller model unless an ablation shows the 72B model is worth the cost.

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

### 10.3 Path A — Three-Gate Quality Validation (when enabled)

Every translated chunk must pass all three gates.

- **Gate A — Named Entity Preservation:** `preserved_entities / original_entities` ≥ **0.75** (XLM-RoBERTa NER).
- **Gate B — KenLM Perplexity:** below **P85** for the reference model.
- **Gate C — Length Ratio:** translation between **0.60× and 2.0×** the original token count.

**Second-chance rescue:** chunks failing Gate B but passing A and C are re-scored against the permissive web KenLM (accept if ≤ P90); otherwise the *original* code-mixed chunk is rerouted to Path B.

### 10.4 Path B — Native Code-Mixed Preservation `NOVEL`

Chunks retained in their original code-mixed form. Quality gates:

1. **Minimum structure:** ≥ 2 complete sentences.
2. **Content density:** ≥ 10 unique non-stop content words.
3. **Safety filter:** BanglaBERT safety classifier (code-mixed social text has elevated baseline toxicity).

### 10.5 Provenance & Corpus Caps

All code-mixed output is tagged with `source_type` (`synthetic_rescue` or `native_code_mixed`), the original Bengali ratio, and the path taken.

**Corpus caps** (enforced at Stage 12):
- Synthetic rescue content: **≤ 15%** of the final corpus (model-collapse guard).
- Native code-mixed content: **≤ 8%** of the final corpus.

---

## 11. Stage 7 — Chunk-Level Deduplication

**Purpose:** A second, finer deduplication pass at *chunk* granularity, using **domain-aware thresholds**. Global document-level dedup already happened at Stage 2; this pass catches duplicated chunks that survived inside otherwise-unique documents (e.g. a boilerplate paragraph repeated across many articles) and deduplicates the rescued/preserved code-mixed chunks.

### 11.1 Domain-Calibrated MinHash LSH

`datasketch` MinHash LSH, 256 permutations, with domain-specific Jaccard thresholds:

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

### 11.2 Code-Mixed Deduplication

Native code-mixed chunks (Path B) are deduplicated separately at **Jaccard 0.80** — Banglish social content has exceptionally high copy-paste rates.

---

## 12. Stage 8 — Final Quality Control

**Purpose:** PII redaction, content safety filtering, document reassembly, and final length validation.

**Retention target:** ≥95%.

### 12.1 Document Reassembly

Reconstruct documents from surviving chunks (clean path + rescue path + native code-mixed path). Insert gap markers `[...]` between non-adjacent surviving chunks. Apply a final document-level KenLM pass at P93 perplexity.

### 12.2 PII Redaction

- **Regex patterns:** Bangladesh NID, mobile numbers (+880), email, bank accounts, passport (A-XXXXXXX).
- **NER-based PII:** person name + street address via fine-tuned BanglaBERT NER.
- **Redaction strategy:** replace with typed placeholders (`[PHONE]`, `[NID]`, `[EMAIL]`, `[ADDRESS]`) rather than deleting, preserving sentence structure.

### 12.3 Content Safety Filtering

- Bengali keyword blocklist for hate speech, communal slurs, explicit content, violent incitement.
- **BanglaBERT safety classifier** on any keyword-triggered document.
- Drop threshold: confidence > **0.75** for harmful content (elevated to reduce false positives on legitimate news/legal text).

### 12.4 Final Length Validation

Drop documents below **150 tokens** after all processing.

### 12.5 Synthetic Proportion Audit

Documents where >50% of tokens originate from LLM rescue → flag and sample 200 for manual human review before inclusion.

---

## 13. Stage 9 — Evaluation Set Decontamination `SOTA`

**Purpose:** Prevent evaluation-set leakage into the pre-training corpus. Follows GPT-3 (Brown et al., 2020) and RefinedWeb (Penedo et al., 2023).

**Retention target:** ≥98%.

### 13.1 Decontamination Blocklist

Collect all evaluation datasets planned for benchmarking:

- TituLLMs BLUB benchmark (5 sets)
- BanglaBERT original test sets
- NCTB textbook test material
- BanglaMath evaluation set
- DIALTSA-BN dialectal evaluation sets
- BnSentMix code-mixed evaluation set
- Custom BanglaLM-Eval prompt-response pairs

**Reserve held-out sets before decontamination** so that evaluation splits are never touched by the corpus. Extract all **13-gram** sequences from every evaluation document.

### 13.2 Contamination Removal

For each surviving document, compute its 13-gram set. If it shares **≥ 3 unique 13-grams** with the blocklist, drop it. This threshold balances false positives (common Bengali phrases) against genuine contamination.

---

## 14. Stage 10 — Tokenizer Co-Design & Fertility Validation `NOVEL`

**Purpose:** Train a Bengali-optimized tokenizer and validate its quality against the corpus *before* model training. The tokenizer is a first-class pipeline output.

**Rationale:** BengaliBPE (2025) showed morphology-aware merges produce better subword units; banglaLlama achieved 2.1× compression over LLaMA's tokenizer; IndicSuperTokenizer showed up to 39.5% fertility improvement with language-specific pre-tokenization.

### 14.1 Tokenizer Architecture

**Type:** Morphology-aware BPE via SentencePiece with Bengali-specific modifications.

- **Grapheme-cluster-aware initialization:** seed the vocabulary with complete Bengali grapheme clusters (base consonant + matra + optional virama). Never split a *juktakkhor* mid-cluster. Example: "ক্ষ" is one token, not "ক" + "্" + "ষ".
- **Morphology-aware merge priority:** prioritize merges producing meaningful Bengali units — verb inflections (`-ছে`, `-ছিল`, `-বে`, `-তে`), case markers (`-র`, `-ের`, `-কে`), postpositions (`থেকে`, `পর্যন্ত`, `দিয়ে`), derivational suffixes (`-কারী`, `-ময়`, `-শীল`).
- **Vocabulary size:** 64,000 tokens.
- **Character coverage:** 0.9999.

### 14.2 Fertility Benchmarking Gate `NOVEL`

Before acceptance, measure **fertility** (tokens per word) across domains and dialects.

| Benchmark | Target (tokens/word) | Failure Threshold |
|-----------|---------------------|-------------------|
| Standard Bengali news | ≤ 1.8 | > 2.5 |
| Bengali literature (prose) | ≤ 2.0 | > 2.8 |
| Bengali legal text | ≤ 2.2 | > 3.0 |
| Code-mixed Banglish | ≤ 2.5 | > 3.5 |
| Dialectal Bengali | ≤ 2.3 | > 3.2 |

If any domain exceeds its failure threshold, retrain with adjusted merge weights. Benchmark against vanilla SentencePiece 64K, Llama-3.2, and (where available) BengaliBPE / IndicSuperTokenizer / TituLLMs tokenizers.

### 14.3 Corpus Re-Tokenization

After validation, re-tokenize the entire corpus to produce the definitive token count. All downstream metrics — domain distribution, caps, epoch planning, model size — are computed from this count.

---

## 15. Stage 11 — Curriculum Scoring & Training Manifest `NOVEL`

**Purpose:** Score every document by difficulty and produce a training-order manifest that sequences data from easy to hard. First application of curriculum learning to Bengali LLM pre-training.

**Rationale:** Large-scale 2026 studies show 18–45% reductions in training steps to baseline with curriculum-ordered data (ACL 2026); DUCL (AAAI 2026) argues ordering must consider both difficulty and utility.

### 15.1 Difficulty Metrics

- **Compression Ratio (CR):** `len(utf8_bytes) / len(zlib.compress(utf8_bytes))`. Higher = more predictable = easier.
- **Lexical Diversity (MTLD):** mean tokens before running type-token ratio drops below 0.72. Higher = harder.
- **Quality & Perplexity:** combines the Stage 3 quality score (low quality → later tier) and the Stage 5 KenLM perplexity rank normalized within domain.

### 15.2 Composite Difficulty Score

```python
difficulty = (
    0.30 * normalized_inverse_CR +          # Low compression = harder
    0.30 * normalized_MTLD +                 # High lexical diversity = harder
    0.25 * normalized_domain_perplexity +    # High perplexity = harder
    0.15 * (1 - quality_score)               # Low quality = later/harder
)
```

### 15.3 Curriculum Tiers

| Tier | Difficulty | Training Window | Content Profile |
|------|-----------|----------------|-----------------|
| **T1 — Foundation** | 0.00–0.25 | Steps 0–25% | Simple news wire, clean high-quality web. Core grammar and vocabulary. |
| **T2 — Expansion** | 0.25–0.50 | Steps 25–50% | Standard prose, educational, encyclopedic. Vocabulary breadth and knowledge. |
| **T3 — Enrichment** | 0.50–0.75 | Steps 50–75% | Literature, legal, medical, technical. Domain complexity. |
| **T4 — Mastery** | 0.75–1.00 | Steps 75–100% | Complex literary prose, code-mixed, dialectal text. Generalization. |

### 15.4 Soft Tier Transitions & Multi-Epoch Interaction `NOVEL`

During the final 10% of each tier's window, blend in the next tier at a 20% ratio to prevent abrupt distribution shift.

**Multi-epoch note (new):** because v2 plans for repeated data (up to ~4 epochs, Section 16), the curriculum tiers repeat each epoch. To avoid verbatim memorization from repetition, shuffle documents *within* each tier differently per epoch and keep the tier *sequence* fixed.

### 15.5 Training Manifest Output

A JSON Lines manifest specifies the exact training order (`doc_id`, `curriculum_tier`, `difficulty_score`, `quality_score`, `domain`, `dialect`, `source_type`, `token_count`). Documents shuffle within a tier; tier order is strict.

---

## 16. Stage 12 — Corpus Assembly, Model Sizing & Audit `BEYOND-SOTA`

### 16.1 Model Sizing — Data-Constrained, Not Chinchilla-Capped `NOVEL`

The model size is chosen from three inputs, not one formula:

```python
unique_tokens      = sum(doc.token_count for doc in final_corpus)   # from Stage 10 re-tokenization
epochs             = min(4, affordable_epochs_given_compute)        # data-constrained scaling
effective_tokens   = unique_tokens * epochs
# Choose the largest model that can be well-trained on effective_tokens
# given the secured training FLOPs, targeting an OVER-trained regime
# (>= 40-100+ tokens/param), NOT the Chinchilla 20:1 point.
```

**Indicative sizing** (assuming ~4 epochs and an over-trained target). The Chinchilla column is shown only to illustrate how much *larger and better-trained* a model the same data supports once repetition is allowed:

| Unique Tokens | Effective (×4) | Chinchilla point (ref only) | Recommended (over-trained) | Indicative Architecture |
|---------------|----------------|-----------------------------|----------------------------|-------------------------|
| 10B | 40B | 500M | 350–500M, well-trained | 24L, 16H, 1024–1280D |
| 15B | 60B | 750M | 500–700M | 24–32L, 16–20H, 1280–1536D |
| 18B | 72B | 900M | 600–800M | 32L, 20H, 1536D |
| 25B+ | 100B+ | 1.25B | 800M–1B+ | 32L, 24H, 2048D |

> The recommended sizes are intentionally *below* the naive Chinchilla point for the effective token count, because an over-trained smaller model is a better, more useful, and cheaper-to-serve foundational artifact than an under-trained larger one. Add the FineWeb2/HPLT/Sangraha backbone to move up this table — more unique data is the real lever.

Compute budget for the training run itself: `FLOPs ≈ 6 × params × effective_tokens`.

### 16.2 Corpus Composition Targets

| Metric | Target |
|--------|--------|
| Total clean tokens | Maximized — no fixed floor |
| Authentic Bengali (natural text) | ≥ 77% |
| Synthetic rescue content | ≤ 15% |
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
| Curriculum: T1 / T2 / T3 / T4 | 20–30 / 25–35 / 20–30 / 10–20% |

### 16.3 Stage-by-Stage Retention & Dedup Report

```
+--------------------------------------------------------------+
|                 SHUDDHIKARAN v2 RETENTION REPORT             |
+--------------------------------------------------------------+
| Stage 0  - Encoding:            raw   -> raw    (100.0%)     |
| Stage 1  - Pre-screening:       raw   -> XX.X   (  >=90%)    |
| Stage 2  - GLOBAL dedup:        XX.X  -> XX.X   ( ~60-80%)   |  <- true unique tokens here
| Stage 3  - Quality scoring:     XX.X  -> XX.X   (100.0%)     |
| Stage 4  - Classification:      XX.X  -> XX.X   (100.0%)     |
| Stage 5  - Chunk processing:    XX.X  -> XX.X   (  >=85%)    |
| Stage 6  - Code-mixed proc:     XX.X  -> XX.X   ( rescue )   |
| Stage 7  - Chunk dedup:         XX.X  -> XX.X   (  ~90%)     |
| Stage 8  - Quality control:     XX.X  -> XX.X   (  >=95%)    |
| Stage 9  - Decontamination:     XX.X  -> XX.X   (  >=98%)    |
|                                                              |
| UNIQUE TOKENS:   ~XX.XB                                      |
| EPOCHS PLANNED:  X   -> EFFECTIVE TOKENS: ~XX.XB             |
| MODEL:           XXXM params (over-trained)                  |
| CONFIG:          XXL-XXH-XXXXD                               |
+--------------------------------------------------------------+
```

### 16.4 Human Quality Audit

Before finalization: 500 documents, stratified across domains/dialects/source types, rated by two native Bengali speakers on fluency (1–5), factual plausibility (1–5), PII-freedom (pass/fail), safety (pass/fail), dialectal authenticity (non-`shuddho` only), and code-mixed naturalness (native code-mixed only). Acceptance: mean fluency ≥ 4.0, mean plausibility ≥ 4.0, zero PII/safety failures.

### 16.5 Provenance Sidecar

Every document carries a JSON Lines sidecar with source, license, domain, dialect, encoding origin, quality score, KenLM perplexity, tokenizer fertility, difficulty score, curriculum tier, and the list of stages passed. This provenance — plus the release of the corpus itself — is the project's most citable contribution (see Section 19).

---

## 17. Engineering & Infrastructure

This section is new in v2 and is treated as part of the specification, because storage and hardware decisions materially affect whether the pipeline finishes on time.

### 17.1 Storage Format

- **Do not write tens of thousands of tiny JSONL files.** The v1 output of ~89K small files caused slow directory operations and I/O overhead. Consolidate output into a **small number of Parquet files with zstd compression** (or sharded gzipped JSONL, ~1–5 GB each). This is faster to read, compresses better, and is native to the HuggingFace `datasets` tooling used for training.

### 17.2 Filesystem Placement

- Keep hot, in-process data on a **Linux-native filesystem (ext4)**, not on a mounted NTFS drive. The observed ~2× slowdown came from WSL2 ↔ NTFS access over the 9P protocol.
- **Current plan (acknowledged):** raw data is staged on HDD now and will be moved to the DGX Spark's NVMe when available. This is fine for storage; run the heavy processing passes on the NVMe (or in the cloud), not directly against the HDD/NTFS mount.

### 17.3 Training Hardware — A Realistic Plan

- The **DGX Spark (GB10, 128 GB unified memory)** is excellent for *building and running the pipeline*: encoding, dedup, the quality classifier, KenLM, tokenizer training, classification, evaluation, fine-tuning, and inference. Use it for everything *around* training.
- The Spark is **not** a from-scratch pretraining rig for a 500M–1B model over tens of billions of tokens — that will be very slow. **Plan to rent a short H100/A100 multi-GPU run for the actual pretraining**, then bring the checkpoint back to the Spark for fine-tuning and serving.
- Budget the run explicitly with `FLOPs ≈ 6 × params × effective_tokens` (Section 16.1).

### 17.4 Evaluation Harness — Build It First

Wire up the evaluation harness (BLUB, BnSentMix, DIALTSA-BN, BanglaMath) **before** training begins, so that ablation variants (Section 20) can be compared. Reserve all held-out evaluation splits before Stage 9 decontamination.

### 17.5 Recommended Execution Order (given a ~3-month window)

To protect the timeline, build in this order and treat the elaborate rescue machinery as optional:

1. Download backbone: **FineWeb2 (bn) + Sangraha** (HPLT already obtained; also pull HPLT buckets 1–4).
2. Normalize all sources to the unified schema (Stage 0) and run Stage 1.
3. Run the **global dedup pass (Stage 2)** — this yields the true token count and tells you if you have enough data for your target size.
4. Train and apply the **quality classifier (Stage 3)**.
5. Classification, chunk processing, chunk dedup, QC, decontamination.
6. Tokenizer + curriculum + assembly.
7. Small **proof-of-concept training run** to prove the data works, then scale up on rented GPUs.
8. Add LLM code-mixed rescue and the multi-KenLM fleet **only if** ablations show they help and time remains.

---

## 18. Comparison with Published Approaches

| Dimension | TituLLMs (ACL 2025) | banglaLlama (LoResLM 2026) | FineWeb2 (2025) | SHUDDHIKARAN v2 |
|-----------|--------------------|-----------------------------|-----------------|-----------------|
| **Filtering granularity** | Document | Document | Language-adaptive | Chunk-level + document quality score |
| **Quality signal** | Heuristics | CulturaX filters | Model-based (edu classifier) | Model-based classifier + KenLM |
| **LID threshold** | 95% | Not reported | Adaptive | 70% doc / 85% chunk |
| **Deduplication** | Flat | Not reported | Per-language | Global early pass + per-domain chunk pass |
| **Backbone source** | CulturaX | CulturaX | Self (WARC) | FineWeb2 + HPLT + Sangraha (WARC) |
| **Code-mixed handling** | Dropped | Passive | N/A | Preserve-default; optional restricted LLM rescue |
| **Dialect awareness** | None | None | None | 6-dialect detection and routing |
| **Tokenizer integration** | Post-hoc | Independent | N/A | Pipeline stage with fertility gates |
| **Curriculum learning** | None | None | N/A | 4-tier, multi-epoch aware |
| **Eval decontamination** | Not reported | Not reported | Not reported | 13-gram blocklist |
| **Model sizing** | Fixed (1B, 3B) | Fixed (8B) | N/A | Data-constrained, over-trained |
| **Synthetic cap** | ~16% | N/A | N/A | ≤ 15% with collapse citation |
| **Provenance** | Minimal | Minimal | Dataset-level | Per-document sidecar |

---

## 19. Novel Contributions

1. **Retention-first pipeline for low-resource languages** — every threshold justified against data-loss cost; scoring and second-chance routing over hard drops.
2. **Global-early deduplication** — a single cross-source exact + fuzzy pass before any expensive compute, yielding an auditable true unique-token count.
3. **Model-based quality scoring used as a ranker, not a filter** — combining FineWeb-Edu/DCLM-style learned quality with retention-first philosophy.
4. **Domain-conditional KenLM with dialect-aware fallback** (ablation-gated fleet).
5. **Dual-path code-mixed processing** with preserve-by-default and optional restricted LLM rescue.
6. **Joint domain–dialect classification** — first Bengali pipeline to combine topical and regional labels.
7. **Morphology-aware tokenizer as a pipeline stage** with fertility pass/fail gates.
8. **Bengali curriculum learning** with multi-epoch-aware tier repetition.
9. **Data-constrained, over-trained model sizing** — size chosen from unique tokens × achievable epochs × secured compute, not a fixed Chinchilla ratio.
10. **Per-stage retention + per-source dedup reporting** with halt-on-anomaly.
11. **An openly-released, deduplicated, quality-scored Bengali corpus with per-document provenance** — the most durable and citable output of the project.
12. **Built-in ablation framework** — the first systematic data-quality study for Bengali.

---

## 20. Ablation Framework

Each variant produces a separate corpus with a unique version tag. Training identically on each and evaluating on standardized benchmarks (BLUB, BnSentMix, DIALTSA-BN, BanglaMath) produces the first systematic Bengali data-quality study.

| Variant ID | Modification | Research Question |
|-----------|-------------|-------------------|
| `abl-no-quality-classifier` | Skip Stage 3 | How much does the learned quality classifier help? |
| `abl-quality-hard-filter` | Stage 3 drops bottom 30% instead of ranking | Is ranking better than hard-dropping low-quality text? |
| `abl-aggressive-lid` | Stage 1 LID at 95% | How much valid data does aggressive LID discard? |
| `abl-multi-kenlm` | Use the 8-model KenLM fleet in Stage 5 | Does domain-conditional KenLM beat a single model? |
| `abl-no-second-chance` | Remove second-chance paths | Does second-chance rescue improve performance? |
| `abl-no-global-dedup` | Skip Stage 2 (chunk dedup only) | How critical is early global dedup? |
| `abl-flat-chunk-dedup` | Stage 7 flat Jaccard 0.85 | Does domain-adaptive dedup outperform flat? |
| `abl-rescue-off` | Stage 6 drops all code-mixed | Is code-mixed processing worth it? |
| `abl-rescue-restricted` | Stage 6 LLM-rescues only high-value subset | Is restricted LLM rescue better than none / than full? |
| `abl-preserve-all-codemixed` | Stage 6 preserves 100%, no translation | Is native preservation sufficient? |
| `abl-no-curriculum` | Random document ordering | Does curriculum learning help Bengali? |
| `abl-reverse-curriculum` | Hard → easy ordering | Is easy-to-hard the correct direction? |
| `abl-no-dialect` | Drop all non-`shuddho` text | Does dialect exposure help or hurt standard tasks? |
| `abl-single-epoch` | Train 1 epoch vs. 4 | Does multi-epoch (data-constrained) training help at this scale? |
| `abl-vanilla-tokenizer` | Vanilla SentencePiece | How much do morphology-aware tokenizer mods matter? |

---

## 21. References

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

---

*SHUDDHIKARAN Pipeline — Version 2.0 (Final)*
