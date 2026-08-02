# SHUDDHIKARAN v3.0 — Implementation Guide for NVIDIA DGX Spark

**Companion to:** `SHUDDHIKARAN_FINAL_PIPELINE.md` (v3.0)
**Target system:** NVIDIA DGX Spark — 20-core Arm CPU, 128 GB unified memory, 4 TB NVMe, aarch64
**Scope:** Stage −1 through Stage 12 execution on a single Spark. Pretraining itself runs on rented GPUs.

---

## 1. Hardware Reality

### 1.1 Actual Specification

| Property | Value | Planning consequence |
|----------|-------|----------------------|
| CPU | **20-core Arm** (10× Cortex-X925 + 10× Cortex-A725) | ~16 usable worker processes, not 60+ |
| Memory | **128 GB unified LPDDR5X** (CPU + GPU share one pool) | GPU allocations directly reduce CPU headroom |
| Memory bandwidth | ~273 GB/s | Shared between CPU and GPU stages |
| GPU | GB10 Grace Blackwell | Excellent for classifier/LLM inference and small-model training |
| Storage | **4 TB NVMe** | ~3 TB safely usable (Section 3) |
| Architecture | **aarch64** | Wheel availability must be verified before scheduling |

### 1.2 The Three Assumptions That Break v1/v2 Plans

**1. It is not a 72-core system.** Any plan built on 72 Grace cores over-estimates CPU-stage throughput by roughly 3.5×. Rebuild every schedule around ~16 parallel workers.

**2. Unified memory is not RAM + VRAM.** When the quality classifier or an LLM occupies 20 GB of GPU memory, that 20 GB is gone from the CPU pool. Never plan a CPU stage and a GPU stage to peak simultaneously unless their combined budget fits under the cap in Section 2.

**3. Heterogeneous cores are not equal.** The 10 performance cores do the real work; the 10 efficiency cores are noticeably slower. Uniform work-splitting across 20 workers means the slowest shard sets the wall time. Use a **work queue with small shards**, never a static split.

### 1.3 What the Spark Is and Is Not For

| Runs well on the Spark | Rent GPUs instead |
|------------------------|-------------------|
| Stages −1 through 12 (the entire data pipeline) | Full pretraining of a 500M–1B model |
| Quality classifier training and inference | Long multi-epoch runs over 40B+ tokens |
| KenLM training and scoring | |
| Tokenizer sweeps | |
| Domain/dialect classifier fine-tuning | |
| Pilot models (100–200M params, a few B tokens) | |
| Evaluation harness, fine-tuning, serving | |

---

## 2. Memory Budget

### 2.1 Fixed Allocation Policy

Treat 128 GB as a hard ceiling with reservations carved out first:

| Allocation | Budget | Rationale |
|------------|--------|-----------|
| OS + system | 8 GB | Non-negotiable |
| Page cache / filesystem | 16 GB | Parquet reads collapse without it |
| Safety headroom | 12 GB | Fragmentation and spikes |
| **Available to pipeline work** | **~92 GB** | Split between CPU and GPU stages |

### 2.2 Per-Stage Memory Ceilings

Enforce these with `cgroups` (or `systemd-run --scope -p MemoryMax=`) so a runaway worker is killed instead of triggering an OOM that takes down the run:

| Stage | Mode | Workers | Per-worker cap | Total cap |
|-------|------|---------|----------------|-----------|
| −1 Benchmark | CPU | 8 | 3 GB | 24 GB |
| 0 Encoding | CPU | 14 | 3 GB | 42 GB |
| 1 Pre-screening | CPU | 14 | 4 GB | 56 GB |
| 2a Exact dedup | CPU + disk sort | 8 | 6 GB | 48 GB |
| 2b MinHash signatures | CPU | 14 | 4 GB | 56 GB |
| 2c Band partition + group | CPU + disk sort | 6 | 8 GB | 48 GB |
| 2d Verify + union-find | CPU | 4 | 12 GB | 48 GB |
| 3 Quality scoring | CPU (fastText) | 12 | 4 GB | 48 GB |
| 4 Classification | GPU | 1–2 | GPU 24 GB | 40 GB total |
| 5 Chunking | CPU | 14 | 3 GB | 42 GB |
| 5 KenLM scoring | CPU | 10 | 6 GB (mmap model) | 60 GB |
| 6 LLM rescue (optional) | GPU | 1 | GPU 60 GB | 75 GB total |
| 7 Chunk dedup | CPU + disk sort | 6 | 8 GB | 48 GB |
| 8 QC (NER, safety) | GPU | 1–2 | GPU 20 GB | 36 GB |
| 9 Decontamination | CPU + disk sort | 8 | 6 GB | 48 GB |
| 10 Tokenizer training | CPU | 1 | 48 GB | 48 GB |
| 11 Curriculum scoring | CPU | 14 | 3 GB | 42 GB |
| 12 Assembly | CPU | 8 | 6 GB | 48 GB |

**Rule: never run a GPU stage concurrently with a high-memory CPU stage.** Serialize Stage 4/6/8 against Stage 2/5/7/10.

### 2.3 KenLM Memory

Load KenLM with **memory mapping**, not full load, and share one mmap across workers:

```python
import kenlm
model = kenlm.Model("kenlm/bn_general.binary")  # trie + mmap, shared pages
```

Build the model as a **quantized trie** (`build_binary -a 22 -q 8 -b 8 trie`). A full-load 5-gram model on a large Bengali corpus can exceed 30 GB; the quantized trie is typically a fraction of that. If the multi-KenLM fleet ablation runs, load **one model at a time** — eight concurrent models will not fit.

---

## 3. Storage Budget for 4 TB

### 3.1 Allocation

Reserve 25% free at all times. Usable working space: **~3.0 TB**.

| Zone | Path | Budget | Retention policy |
|------|------|--------|------------------|
| Raw archive (compressed) | `/data/00_raw/` | 700 GB | Read-only; keep until Stage 2 validated |
| Stage outputs (Parquet+zstd) | `/data/stage_XX/` | 1,200 GB | Keep last 2 stages; archive older to external |
| Dedup scratch (signatures, bands, sorts) | `/scratch/dedup/` | 700 GB | Delete after Stage 2/7 `_SUCCESS` |
| Models, tokenizers, KenLM | `/data/models/` | 150 GB | Permanent |
| Manifests, metrics, samples, quarantine | `/data/meta/` | 100 GB | Permanent — never delete |
| Final corpus + tokenized shards | `/data/final/` | 150 GB | Permanent |
| **Free reserve** | — | **1,000 GB** | Never allocate |

### 3.2 Why Dedup Scratch Is So Large

For **N** documents with **P** permutations and **B** bands:

| Artifact | Size formula | At N = 50M, P = 128, B = 16 |
|----------|--------------|------------------------------|
| MinHash signatures | `N × P × 4 B` | ~26 GB |
| Band keys | `N × B × 24 B` | ~19 GB |
| External sort temp | ~2× the input being sorted | ~40–80 GB |
| Candidate pairs | Data-dependent, can explode | 50–300 GB |
| Verification shingle sets | Streamed, bounded | ~50 GB peak |

**128 permutations instead of 256 halves the signature and I/O cost.** Benchmark both at Stage −1 (ablation `abl-dedup-perms`) before committing; recall gain from 256 is frequently marginal.

**Cap bucket sizes.** A single band bucket containing 500k documents means boilerplate, not duplication. Log and truncate oversized buckets rather than generating a quadratic candidate explosion.

### 3.3 Storage Discipline

- **Never delete a stage's input** until the next stage has written `_SUCCESS` and checksums verify.
- **Parquet + zstd level 3** everywhere; level 9 only for the final archive.
- **Target 1–5 GB per Parquet file.** The v1 output of ~89K tiny JSONL files was an I/O pathology.
- **Monitor free space in the stage gate.** Abort a stage before it fills the disk, not during.

```bash
# Pre-flight check, run by every stage runner
avail=$(df --output=avail -BG /data | tail -1 | tr -dc '0-9')
[ "$avail" -lt 1000 ] && { echo "ABORT: reserve breached (${avail}G)"; exit 1; }
```

---

## 4. Environment Setup

### 4.1 aarch64 Dependency Verification — Do This First

Verify every wheel builds or installs on arm64 **during Stage −1**, before the schedule depends on it. A missing `kenlm` wheel discovered in month two is a schedule failure.

```bash
sudo apt-get update && sudo apt-get install -y \
  build-essential cmake git \
  libboost-all-dev libeigen3-dev zlib1g-dev libbz2-dev liblzma-dev \
  zstd python3-dev

curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv --python 3.11 /opt/shuddh/venv
source /opt/shuddh/venv/bin/activate

uv pip install \
  pyarrow pandas numpy polars \
  fasttext-wheel sentencepiece tokenizers datasets \
  regex unicodedata2 tqdm pyyaml orjson xxhash \
  scikit-learn

# KenLM: expect a source build on aarch64
git clone https://github.com/kpu/kenlm /opt/kenlm
cmake -S /opt/kenlm -B /opt/kenlm/build -DCMAKE_BUILD_TYPE=Release
cmake --build /opt/kenlm/build -j 16
uv pip install /opt/kenlm
```

Record every resolved version in `environment.lock` and hash it into `processing_version`.

### 4.2 Verification Script

```python
# scripts/verify_env.py
import platform, importlib
assert platform.machine() == "aarch64", platform.machine()

for mod in ["pyarrow","numpy","polars","fasttext","sentencepiece",
            "tokenizers","kenlm","xxhash","sklearn"]:
    m = importlib.import_module(mod)
    print(f"[ok] {mod} {getattr(m,'__version__','?')}")

import torch
print("[ok] torch", torch.__version__, "cuda", torch.cuda.is_available())
```

### 4.3 System Tuning

```bash
# Filesystem: ext4 or xfs on NVMe. Never process on an NTFS mount.
sudo mount -o noatime,nodiratime /dev/nvme0n1p1 /data

# Reduce swap pressure on unified memory
sudo sysctl -w vm.swappiness=10
sudo sysctl -w vm.dirty_ratio=10
sudo sysctl -w vm.dirty_background_ratio=5

# Raise file descriptor limits for external sorts
ulimit -n 65535
```

---

## 5. Concurrency Model

### 5.1 Work Queue, Not Static Split

Because 10 cores are fast and 10 are slow, a static split leaves the fast cores idle waiting on the slow ones. Use small shards and a queue.

```python
# scripts/runner.py
import os, json, hashlib, tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

MAX_WORKERS = 14  # 20 cores minus OS, I/O, and monitoring headroom

def atomic_write_parquet(table, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    import pyarrow.parquet as pq
    with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False, suffix=".tmp") as tmp:
        pq.write_table(table, tmp.name, compression="zstd", compression_level=3)
    os.replace(tmp.name, dest)   # atomic on the same filesystem

def run_stage(stage_fn, shards, out_dir: Path, config: dict, workers=MAX_WORKERS):
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]
    metrics, done = [], 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(stage_fn, s, out_dir, config): s
            for s in shards
            if not (out_dir / f"{s.stem}.parquet").exists()   # resumability
        }
        for fut in as_completed(futures):
            shard = futures[fut]
            try:
                metrics.append(fut.result())
                done += 1
            except Exception as e:
                # One bad shard must not kill the stage
                (out_dir / "failed").mkdir(exist_ok=True)
                (out_dir / "failed" / f"{shard.stem}.err").write_text(repr(e))

    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    if not (out_dir / "failed").exists():
        (out_dir / "_SUCCESS").write_text(json.dumps({"config_hash": cfg_hash, "shards": done}))
    return metrics
```

### 5.2 Shard Sizing

| Stage class | Shard size | Reason |
|-------------|-----------|--------|
| Encoding, screening, chunking | 200k docs (~1–2 GB) | Fits per-worker cap, fast retry |
| MinHash signatures | 200k docs | Bounded memory |
| Band partitions | ~2 GB per partition | Fits an in-memory sort per worker |
| GPU inference | Batch-tuned, 1–2 processes | GPU memory is the constraint |
| Tokenizer training | Single process, sampled corpus | Not parallelizable |

### 5.3 Memory-Capped Launch

```bash
systemd-run --scope -p MemoryMax=56G -p CPUQuota=1400% \
  python scripts/run_stage.py --stage 1 --workers 14
```

---

## 6. Stage-by-Stage Execution

### 6.1 Stage −1 — Governance & Benchmark

```bash
python -m shuddh.stage_neg1.registry     --sources conf/sources/           # YAML records
python -m shuddh.stage_neg1.checksums    --out /data/meta/manifests/       # SHA-256 per file
python -m shuddh.stage_neg1.golden       --per-source 800                  # frozen samples
python -m shuddh.stage_neg1.freeze_eval  --out /data/meta/eval_registry/   # eval sets frozen
python -m shuddh.stage_neg1.benchmark    --fraction 0.01 --min-docs 1000000
```

The benchmark report is the input to every capacity plan that follows. Do not proceed without it.

### 6.2 Stage 0 — Encoding

- 14 workers, 3 GB cap, 200k-doc shards.
- Bijoy conversion is **candidate-scored** (pipeline Section 5.1): run all candidates, score Bengali validity, accept only on a clear margin, quarantine otherwise.
- Emit `accepted/`, `quarantine/`, and `encoding_operations` per document.
- Run against golden samples first; diff the output before the full pass.

### 6.3 Stage 1 — Pre-screening

- 14 workers, 4 GB cap. Load fastText LID **once per worker**, not per document.
- Emit the composite routing score and all component signals into the schema.
- Stage gate: per-source retention, document- and token-weighted, with halt below 85%.

### 6.4 Stage 2 — Global Dedup (the critical stage)

Run as four sub-stages, each independently resumable:

```bash
# 2a Exact dedup: hash -> external sort -> group
python -m shuddh.stage2.exact_hash    --workers 14 --out /scratch/dedup/hashes/
sort -S 8G -T /scratch/sort --parallel=8 -k1,1 /scratch/dedup/hashes/*.tsv \
  > /scratch/dedup/hashes_sorted.tsv
python -m shuddh.stage2.exact_group   --in /scratch/dedup/hashes_sorted.tsv

# 2b MinHash signatures (shard-parallel, bounded memory)
python -m shuddh.stage2.minhash --perms 128 --shingle word5 --seed 1337 --workers 14

# 2c Band keys -> partition -> sort/group per partition
python -m shuddh.stage2.bands --bands 16 --rows 8 --partition-size 2G --workers 6

# 2d Verify candidates against real shingle sets, then union-find clusters
python -m shuddh.stage2.verify_cluster --jaccard 0.85 --workers 4

# 2e Canonical selection (rights-aware) and cluster provenance
python -m shuddh.stage2.canonical --policy rights_first
```

Critical implementation notes:

- **Never build an in-memory LSH index.** The 51 GB signature estimate excludes object overhead, buckets, candidate pairs, and page cache; the real requirement is multiples of that and will not fit in 128 GB alongside anything else.
- Use `sort -S 8G -T /scratch/sort --parallel=8` for external sorts; do not exceed 8 GB per sort process.
- Cap and log oversized band buckets.
- Verify every candidate pair with exact Jaccard on shingle sets.
- Retain `cluster_source_ids` for all contributors.
- Honor the Section 7.5 carve-outs (dialect variants, translations, poems, statutes, canonical religious/literary text).
- Delete `/scratch/dedup/` only after `_SUCCESS` and report validation.

### 6.5 Stage 3 — Quality Scoring

- fastText inference: 12 CPU workers, ~4 GB each. GPU not required.
- Training data must follow the leakage controls in pipeline Section 8.2 — positives and negatives from the *same* sources, split by hostname.
- Store the score plus model ID and version.

### 6.6 Stage 4 — Domain & Dialect (GPU)

- Serialize against CPU-heavy stages.
- 1–2 processes, GPU capped at 24 GB, fp16, batch size tuned from the benchmark.
- Emit full distributions and an `unknown` class; never default to `shuddho`.

### 6.7 Stage 5 — Chunking & KenLM

- Chunking: 14 workers, cheap. Preserve `char_span` and `discontinuity_before`; emit **no literal `[...]` markers**.
- KenLM: 10 workers over a shared quantized-trie mmap. Default mode is **scoring**, not gating.

### 6.8 Stage 6 — Code-Mixed (GPU, optional)

- If LLM rescue is enabled: vLLM with continuous batching, single process, GPU capped at 60 GB, all other stages stopped.
- Record model ID, prompt hash, and decoding parameters per generation. Always keep the original chunk.
- Route romanized Bengali here rather than dropping it.

### 6.9 Stage 7 — Chunk Dedup

Run cheap-first and stop when marginal benefit disappears:

```bash
python -m shuddh.stage7.exact_chunk        --workers 14
python -m shuddh.stage7.boilerplate_lines  --min-host-docs 20 --workers 12
python -m shuddh.stage7.host_templates     --workers 12
# Fuzzy only where the labelled pair set validated a threshold:
python -m shuddh.stage7.fuzzy --domains news,social,web_general --workers 6
```

Chunk count exceeds document count, so this is the most expensive dedup pass. Steps 1–3 remove most volume at a fraction of the cost.

### 6.10 Stages 8–9 — QC and Decontamination

- PII/safety NER on GPU, capped at 20 GB, serialized against CPU stages.
- Apply the tiered PII policy: keep public figures, redact contact details and sensitive identifiers.
- Decontamination: 13-gram index built as an external sort, plus MinHash similarity and field-wise matching. Emit the per-benchmark contamination report.

### 6.11 Stage 10 — Tokenizer Sweep

- Train on a **sampled** corpus (10–20 GB of text), not the full corpus — SentencePiece memory scales with input.
- Single process, 48 GB cap, `--input_sentence_size` with shuffling enabled.
- Run the full sweep (unigram/BPE × 32K/48K/64K × byte-fallback on/off) sequentially; each run is hours, not days, at this sample size.
- Measure fertility, byte-fallback rate, sequence-length distribution, and **grapheme fragmentation**.

### 6.12 Stages 11–12 — Curriculum, Assembly, Pilots

- Curriculum scoring: 14 CPU workers, cheap.
- Assembly emits both the **training view** and the **release view**.
- Pilot models (100–200M params, a few B tokens) train on the Spark to produce the epoch-degradation curve, tokenizer decision, and curriculum-arm comparison.
- Full pretraining moves to rented H100/A100 nodes.

---

## 7. Monitoring

### 7.1 Continuous Watch

```bash
# Unified memory, per-process
watch -n 5 'free -g; ps -eo pid,rss,pcpu,comm --sort=-rss | head -15'

# GPU
nvidia-smi dmon -s pucm

# Disk
watch -n 30 'df -h /data /scratch; du -sh /scratch/dedup/* 2>/dev/null'

# I/O saturation
iostat -x 5
```

### 7.2 Abort Conditions

| Condition | Action |
|-----------|--------|
| Free space on `/data` < 1 TB | Abort before the next shard |
| Total RSS > 100 GB | Pause the queue, drain in-flight workers |
| Swap in use > 2 GB | Reduce workers — unified memory is thrashing |
| Throughput < 50% of benchmark | Stop and investigate; do not "wait it out" |
| Candidate pairs > 10× benchmark prediction | Bucket explosion — cap buckets and rerun 2c |
| Any stage retention deviates > 15% from benchmark | Halt at the stage gate |

### 7.3 Metrics Every Stage Must Emit

```json
{
  "stage": 2,
  "substage": "2d_verify_cluster",
  "run_id": "2026-02-01T09-00Z-a41f9c",
  "config_hash": "sha256:...",
  "code_commit": "git:9f21ab3",
  "environment_lock": "sha256:...",
  "docs_in": 0, "docs_out": 0,
  "tokens_in": 0, "tokens_out": 0,
  "bytes_in": 0, "bytes_out": 0,
  "quarantined": 0,
  "retention_docs": 0.0, "retention_tokens": 0.0,
  "per_source": {},
  "drop_reasons": {},
  "wall_seconds": 0,
  "docs_per_second": 0.0,
  "peak_rss_gb": 0.0,
  "peak_gpu_gb": 0.0,
  "disk_free_gb_start": 0, "disk_free_gb_end": 0
}
```

---

## 8. Testing

| Level | What it covers | When it runs |
|-------|----------------|--------------|
| Unit | Normalization, chunk boundaries, hashing, band math | Every commit |
| Property | NFC idempotence; chunk spans reassemble to the original; dedup is order-independent | Every commit |
| Golden sample | Every parser and threshold against the frozen Stage −1 samples | Every commit |
| Dedup validation | Precision/recall on the labelled pair set | Before any threshold change |
| Benchmark regression | 1% slice throughput and retention vs. the recorded baseline | Before every full stage run |
| End-to-end | Full pipeline on the 1% slice | Before each phase |

Two property tests worth writing first, because both catch silent corpus corruption:

```python
def test_nfc_idempotent(text):
    once = normalize(text)
    assert normalize(once) == once

def test_chunks_reassemble(doc):
    chunks = chunk_document(doc)
    assert "".join(doc.text[c.start:c.end] for c in chunks) == doc.text
```

---

## 9. Failure Recovery

| Failure | Recovery |
|---------|----------|
| OOM kill | Lower `--workers` by 4, rerun; completed shards are skipped |
| Disk full mid-stage | Delete `/scratch/dedup/` from a completed stage; never delete unvalidated output |
| Corrupt Parquet | Delete that part file only; the runner re-emits it |
| Candidate-pair explosion | Cap bucket size, raise the Jaccard target, rerun 2c–2d |
| Power loss | Atomic writes + `_SUCCESS` mean the stage resumes from the last complete shard |
| Wrong threshold discovered late | Reprocess from the last `_SUCCESS` stage; quarantine makes previously-borderline data recoverable |
| GPU OOM | Reduce batch size; confirm no CPU stage is running concurrently |

**Non-negotiable:** keep `/data/meta/` (manifests, metrics, quarantine, samples) on separate physical storage or backed up externally. If the NVMe fails, the corpus can be rebuilt from raw sources — the provenance and audit trail cannot.

---

## 10. Capacity Planning Worksheet

Fill this in from the Stage −1 benchmark. Do not commit to a schedule with blanks.

| Quantity | Symbol | Measured value | Source |
|----------|--------|----------------|--------|
| Total raw compressed size | `S_c` | ____ GB | Registry |
| Expansion ratio (uncompressed / compressed) | `E` | ____ × | Benchmark |
| Total documents after Stage 1 | `N_1` | ____ M | Benchmark × scale |
| Stage 2 document retention | `r_2` | ____ | Benchmark |
| Unique documents | `N_2 = N_1 · r_2` | ____ M | Derived |
| Chunks per document | `k` | ____ | Benchmark |
| Bytes per token (provisional tokenizer) | `b` | ____ | Benchmark |
| Unique tokens | `T = (S_c · E · r_2) / b` | ____ B | Derived |
| Validated epochs | `e` | ____ | Pilot (≤ 4) |
| Effective tokens | `T · e` | ____ B | Derived |
| Recommended model size | — | ____ M params | Pipeline §17.1 |
| Pretraining FLOPs | `6 · P · T · e` | ____ | Derived |

Storage check: peak usage must satisfy

$$S_{\text{peak}} \approx S_c + (S_c \cdot E \cdot 0.35)_{\text{parquet}} + S_{\text{scratch}} + S_{\text{meta}} < 3{,}000\ \text{GB}$$

If it does not, stream from external storage and delete validated intermediates more aggressively — do not reduce the free reserve.

---

## 11. Execution Checklist

**Phase A — Governance & Benchmark**
- [ ] Source registry YAML complete for every source, with `release_class`
- [ ] SHA-256 manifests generated and verified
- [ ] License matrix reviewed; exclusion register written
- [ ] Evaluation sets frozen with checksums
- [ ] Golden samples drawn (500–1,000 per source)
- [ ] aarch64 environment built; `environment.lock` recorded
- [ ] 1% benchmark run; capacity worksheet filled
- [ ] Storage layout created with the 1 TB reserve enforced

**Phase B — Foundation**
- [ ] Versioned readers for every source format
- [ ] Unified schema implemented and contract-validated
- [ ] Atomic writes, `_SUCCESS` manifests, quarantine lanes
- [ ] Unit, property, and golden-sample tests passing
- [ ] Stage-gate runner with abort conditions wired up

**Phase C — Baseline Corpus**
- [ ] Stage 0 encoding (candidate-scored Bijoy, quarantine lane)
- [ ] Stage 1 composite screening
- [ ] Stage 2a exact dedup
- [ ] Dedup labelled pair set built; precision/recall measured
- [ ] Stage 2b–2e external-memory fuzzy dedup + rights-aware canonical selection
- [ ] Stage 3 quality classifier (leakage-controlled, model card written)
- [ ] Stage 5 chunking with spans, no `[...]` markers
- [ ] Stage 7 exact + boilerplate chunk dedup
- [ ] Stage 8 tiered PII and safety policy
- [ ] Stage 9 multi-signal decontamination + per-benchmark report

**Phase D — Enhancements (each ablation-gated)**
- [ ] Domain classifier (tiered) · Dialect classifier (tiered)
- [ ] KenLM gating · Fuzzy chunk dedup · LLM rescue · Curriculum ordering

**Phase E — Tokenizer & Pilots**
- [ ] Tokenizer sweep complete with all metrics
- [ ] Pilot models trained; epoch-degradation curve measured
- [ ] `validated_epochs` set from evidence

**Phase F — Assembly & Decision**
- [ ] Corpus frozen; provenance, dedup, contamination reports published
- [ ] Human audit complete with inter-annotator agreement; rejected/quarantined sample audited
- [ ] Training and release views materialized
- [ ] Model size fitted to secured compute; scaling pilot passed

---

*DGX Spark Implementation Guide — companion to SHUDDHIKARAN v3.0. All capacity figures are placeholders until the Stage −1 benchmark fills the worksheet in Section 10.*
