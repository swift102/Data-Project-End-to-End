# Email Requirement Discovery & Classification — Project Documentation

## 1. Brief project description

A single Microsoft Fabric (Spark) notebook that reads a corpus of banking emails from the
Bronze lakehouse and answers two questions when the set of "requirements" is unknown up front:

1. **Discovery** — *what are people actually asking for?* The notebook groups emails into a
   data-chosen number of recurring request types.
2. **Classification** — *which request does each email (and any future email) belong to?* It
   produces a reusable scorer and writes the results to the Silver lakehouse.

It replaces an earlier BERTopic (UMAP → HDBSCAN) attempt. Density clustering pushed a large
share of short, templated emails into a `-1` outlier bucket (no requirement assigned), was
unstable across runs, and left no deployable classifier behind. The current approach assigns
every email and leaves a callable classifier for new mail.

**Inputs:** `.eml` files under `Files/bronze_raw/banking_data/<YYYY>/<MM>/emails` in
`lh_bronze_banking_data`. Each email reportedly has a `.pdf` twin, so the loader globs `*.eml`
deliberately.

**Outputs:** two Delta tables in `lh_silver_banking_data.dbo`:
- `email_topic_assignments` — one row per email (topic, label, confidence, `needs_review`, template counts).
- `email_topic_summary` — one row per discovered requirement (size, label, plain-language summary, recurring subjects, average confidence).

---

## 2. Pipeline stages

| # | Stage | What it does |
|---|-------|--------------|
| 1 | Load | Reads `.eml` binary files, MIME-parses to `message_id`, `subject`, `body`. |
| 2 | Clean | `clean_email()` strips quoted reply history, sign-offs, disclaimers, raw emails and URLs. Pure function, reused by the template and classifier layers. |
| 3 | Noise filter | Drops auto-replies / OOO / undeliverable / receipts and anything under `MIN_TOKENS` (5). |
| 4 | Template frequency | Normalises subject and body to templates (slot-fills numbers/dates), counts recurrences. A deterministic *literal* signal that cross-checks the clusters. |
| 5 | Embed | Encodes cleaned text to sentence vectors (see §3). |
| 6 | Discover | Spherical K-Means; K swept over `[4, 20]` and chosen by best cosine silhouette. |
| 7 | Classify + validate | Nearest-centroid assignment with confidence/margin scoring; logistic-regression separability check on a 25% hold-out. |
| 8 | Inspect | Prints each requirement's size, recurring subjects, and **medoid** (most central real email). The actual validation step. |
| 9 | Build frames | Assembles `assign_df` and `summary_df`, plus a reusable `classify_requests(subjects, bodies)`. |
| 10 | Write | Single writer; Delta `overwrite` via three-part `saveAsTable` (Silver is not the default lakehouse). |
| 11 | Verify | Read-back row counts and schema; no writes. |

---

## 3. Model used and how it behaves

There are three distinct components. Only the first two touch production scoring; the third is
a diagnostic.

### 3.1 Embedding model (the workhorse)

- **Model:** `all-MiniLM-L6-v2` (sentence-transformers), a 6-layer distilled MiniLM sentence
  encoder producing **384-dimensional** embeddings.
- **Behaviour:** maps each email (subject + cleaned body) to a dense vector positioned so that
  semantically similar requests sit close together in cosine space. It is frozen and used only
  for inference — no fine-tuning.
- **Configurable:** `EMBEDDING_MODEL` can be swapped to
  `paraphrase-multilingual-MiniLM-L12-v2` if the corpus is multilingual.

### 3.2 Discovery + deployed classifier

- **Clustering:** scikit-learn `KMeans` on **L2-normalised** vectors, which makes Euclidean
  K-Means behave like cosine ("spherical" K-Means). `n_init=10`, `random_state=42`. K is the
  value with the highest cosine silhouette across 4–20.
- **Deployed classifier:** **nearest-centroid**, not a trained model. A new email is embedded,
  L2-normalised, and assigned to the centroid with the highest cosine similarity
  (`classify_requests()`). This is what scores future mail.
- **Confidence behaviour:**
  - `confidence` = cosine similarity to the assigned centroid.
  - `margin` = top-1 minus top-2 centroid similarity.
  - An email is flagged `needs_review` when `confidence < 0.35` (`CONF_MIN`) **or**
    `margin < 0.02` (`MARGIN_MIN`) — it abstains rather than silently mislabelling.
- **Determinism:** fixed by `random_state=42`. Changing it is the intended stability test —
  requirements that survive a reseed are real; those that don't were artefacts.
- **No outlier bucket:** unlike the prior HDBSCAN approach, every email is assigned.
- **Naming:** requirements are described by their **medoid** (the most central real email) and
  recurring subjects — deliberately *not* keyword lists.

### 3.3 Logistic regression (diagnostic only)

- scikit-learn `LogisticRegression(C=10, max_iter=1000)` trained on a 75/25 stratified split.
- **Purpose:** measures whether the discovered clusters are *separable* in embedding space.
- **Not deployed.** High held-out accuracy means the groups are cleanly separable — it does
  **not** mean they are the *correct* requirements.

---

## 4. Caveats and what to verify (read before trusting the numbers)

These are limitations of the method and the current data, surfaced rather than glossed over.

1. **No labelled ground truth.** Silhouette and LR accuracy measure internal separability, not
   correctness. The only real validation is a human reading `request_summary` plus the
   recurring subjects (stage 8 and the in-notebook checklist).

2. **The current corpus appears templated/synthetic.** Subjects and bodies are monthly
   variations of a small set of templates spanning 2019–2025. On data like this, separability
   is trivially high — observed `avg_confidence` sits around 0.96–0.99 and the LR check will be
   near-perfect. **These scores do not indicate the classifier will perform this well on real,
   free-text email.** Re-validate on genuine production mail before relying on the metrics.

3. **`topic_label` can mislead.** The label is the *most frequent subject* in a cluster, but
   `dominant_subject_share` is ~0.0125 (≈ 1 in 80), so subjects are effectively unique per
   email. The label is therefore an arbitrary single-month subject and can disagree with the
   cluster's own summary (e.g. a cluster labelled "September 2020 …" whose medoid summary reads
   "February 2024 …"). Treat the label as a handle, not a definition; the `request_summary` is
   the authoritative description.

4. **K is capped at 20.** If the real corpus contains more than 20 distinct asks, raise
   `K_MAX` and re-sweep, or a large `needs_review` tail will appear.

5. **Cleaning is regex-based.** Surviving footers or un-stripped templates can form their own
   confident-but-useless cluster. The inspect step exists to catch these.

---

## 5. Configuration knobs (CONFIG cell)

| Knob | Default | Effect |
|------|---------|--------|
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | Sentence encoder; swap for multilingual. |
| `MIN_TOKENS` | 5 | Minimum cleaned-token count to keep an email. |
| `K_MIN`, `K_MAX` | 4, 20 | Silhouette sweep range for K. |
| `CONF_MIN` | 0.35 | Cosine floor below which an email is `needs_review`. |
| `MARGIN_MIN` | 0.02 | Top1−top2 margin floor for ambiguity. |
| `TEST_SIZE` | 0.25 | Hold-out fraction for the LR separability check. |
| `COMMON_SUBJECT_MIN` / `COMMON_BODY_MIN` | 5 | Recurrence threshold for the template layer. |
| `RANDOM_STATE` | 42 | Seed; vary it for the stability test. |
