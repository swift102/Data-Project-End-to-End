# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3eb590da-d3b9-45f8-8f0e-1171e9ac479e",
# META       "default_lakehouse_name": "lh_bronze_banking_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "3eb590da-d3b9-45f8-8f0e-1171e9ac479e"
# META         },
# META         {
# META           "id": "a03cfff1-048d-457c-8848-da958470832d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 📧 Email Requirement Discovery & Classification
# 
# ## 🎯 Goal
# Discover **what users are requesting** from ingested emails when the requirement space is
# unknown, then **classify** every email (and any future email) into those requirements.
# 
# ## 🧭 Method — and why it changed
# The earlier BERTopic (UMAP → HDBSCAN) approach was the wrong tool for this corpus: density
# clustering dumps a large share of short, templated banking emails into a `-1` *outlier*
# bucket (no requirement assigned), is unstable, and produces no reusable classifier. This
# notebook uses the right-sized approach instead:
# 
# 1. **Embed** emails (MiniLM sentence vectors).
# 2. **Discover** requirements with **spherical K-Means**, **K chosen by silhouette** — every
#    email is assigned, nothing is thrown away.
# 3. **Describe** each requirement in plain language via its **medoid** (the most central real
#    email), plus the actual recurring subjects.
# 4. **Classify** with a **nearest-centroid** rule (deployable on new emails) and validate
#    separability with a held-out **logistic-regression** report. Low-confidence emails are
#    flagged `needs_review` instead of silently mislabelled.
# 
# > **Honest caveat:** with no labelled ground truth, silhouette and LR accuracy only measure
# > whether clusters are *internally separable* — not whether the grouping is *correct*. The
# > real validation is reading the requirement summaries in the INSPECT step.


# CELL ********************

import os
os.environ["HF_HUB_DISABLE_XET"] = "1"

# Slimmed: BERTopic/UMAP/HDBSCAN removed. We need embeddings + scikit-learn only.
%pip install -q "transformers==4.44.2" "sentence-transformers==3.0.1" scikit-learn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ⚙️ CONFIG — The Knobs You Own
# Re-run after any kernel restart. Clustering knobs are now **K-Means + silhouette**, not
# HDBSCAN density thresholds.


# CELL ********************

# --- source columns (must match what the loader produces) ---
ID_COL, SUBJECT_COL, TEXT_COL = "message_id", "subject", "body"

# --- SILVER destination (the writer cell uses the 3-part name lh_silver_banking_data.dbo.*) ---
ASSIGN_TABLE  = "email_topic_assignments"
SUMMARY_TABLE = "email_topic_summary"

# --- boilerplate to strip (inspect, then add real footers only if they leak into clusters) ---
DISCLAIMER_PATTERNS = [
    r"This (e-?mail|message) and any attachments.*",
    r"CONFIDENTIALITY NOTICE.*",
]

# --- embedding ---
EMBEDDING_MODEL = "all-MiniLM-L6-v2"   # multilingual? paraphrase-multilingual-MiniLM-L12-v2
MIN_TOKENS      = 5
RANDOM_STATE    = 42

# --- discovery (K-Means + silhouette sweep) ---
K_MIN, K_MAX    = 4, 20    # silhouette picks the best K here; narrow once you've seen the sweep

# --- classifier confidence / abstention ---
CONF_MIN        = 0.35     # cosine-to-centroid floor; below -> needs_review
MARGIN_MIN      = 0.02     # top1-top2 cosine margin floor; below -> ambiguous
TEST_SIZE       = 0.25     # held-out fraction for the separability check

# --- template-frequency layer ("common" is a judgment call; read the printout, then retune) ---
COMMON_SUBJECT_MIN = 5
COMMON_BODY_MIN    = 5
TOP_N_SUBJECTS     = 3
EXAMPLE_CHARS      = 280
SUMMARY_CHARS      = 200
TOP_N_PRINT        = 15

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📂 2. Load from Fabric Lakehouse Files
# Reads `.eml` from bronze, parses MIME → `message_id`, `subject`, `body`. The `*.eml` glob is
# load-bearing (each email has a `.pdf` twin you do not want).


# CELL ********************

from email import policy
from email.parser import BytesParser
import pandas as pd

try:
    fs = notebookutils.fs
except NameError:
    fs = mssparkutils.fs

BASE = "Files/bronze_raw/banking_data"
SCAN_ALL_MONTHS = True
ONE_MONTH = "2019/02"

def email_dirs():
    if not SCAN_ALL_MONTHS:
        return [f"{BASE}/{ONE_MONTH}/emails"]
    out = []
    for y in fs.ls(BASE):
        if not y.isDir: continue
        for m in fs.ls(y.path):
            p = f"{m.path}/emails"
            try: fs.ls(p); out.append(p)
            except Exception: pass
    return out

dirs = email_dirs()
print(f"{len(dirs)} email folder(s)")

bdf = (spark.read.format("binaryFile")
       .option("pathGlobFilter", "*.eml").load(dirs)
       .select("path", "content").toPandas())
rows = []
for _, r in bdf.iterrows():
    msg = BytesParser(policy=policy.default).parsebytes(r["content"])
    part = msg.get_body(preferencelist=("plain", "html"))
    rows.append({
        "message_id": r["path"].split("/")[-1],
        "subject": msg["subject"] or "",
        "body": part.get_content() if part else "",
    })
pdf = pd.DataFrame(rows)
print(f"{len(pdf):,} emails loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Clean — strip quoted history, signatures, disclaimers
# `clean_email` is reused by the template + classifier layers, so it stays a pure function.


# CELL ********************

import re

REPLY_MARKERS = [
    r"\nOn .{0,120}? wrote:",
    r"\n-{2,}\s*Original Message\s*-{2,}",
    r"\nFrom:\s.*?\nSent:\s.*?\nTo:\s",
    r"\n_{5,}",
    r"\nSent from my \w+",
]
SIGNOFF_RE = re.compile(
    r"\n\s*(kind regards|best regards|regards|thanks|thank you|cheers|sincerely)[,.]?\s*\n",
    re.IGNORECASE,
)
DISCLAIMER_RES = [re.compile(p, re.IGNORECASE | re.DOTALL) for p in DISCLAIMER_PATTERNS]

def clean_email(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cut = len(text)
    for pat in REPLY_MARKERS:
        m = re.search(pat, text)
        if m:
            cut = min(cut, m.start())
    text = text[:cut]
    text = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith(">"))
    for rx in DISCLAIMER_RES:
        text = rx.sub("", text)
    m = SIGNOFF_RE.search(text)
    if m:
        text = text[:m.start()]
    text = re.sub(r"\S+@\S+", " ", text)
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def compose(row):
    subj = (row[SUBJECT_COL] + ". ") if SUBJECT_COL and isinstance(row.get(SUBJECT_COL), str) else ""
    return (subj + clean_email(row[TEXT_COL])).strip()

pdf["clean_text"] = pdf.apply(compose, axis=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Filter noise (auto-replies, OOO, empties)
# Drop obvious non-signal before it forms its own confident, useless cluster.


# CELL ********************

NOISE_RE = re.compile(
    r"\b(out of office|automatic reply|auto-reply|do not reply|delivery (has )?failed|"
    r"undeliverable|read receipt|unsubscribe)\b",
    re.IGNORECASE,
)

pdf["n_tokens"] = pdf["clean_text"].str.split().str.len()
mask = (pdf["n_tokens"] >= MIN_TOKENS) & (~pdf["clean_text"].str.contains(NOISE_RE))
dropped = (~mask).sum()
pdf = pdf[mask].reset_index(drop=True)
print(f"Dropped {dropped:,} noise/short emails -> {len(pdf):,} remain")

docs = pdf["clean_text"].tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4b. Template frequency — is the subject common? is the message common?
# The **literal** signal: normalize each subject/body to a template, count recurrences. For
# templated synthetic data this is often the truest intent signal, and it cross-checks the
# clusters. Raw values are kept beside normalized ones so you can audit the cutoff.


# CELL ********************

# --- normalizers: collapse slot-filled values so templates line up ---------------------
_RE_PREFIX = re.compile(r"^\s*(re|fwd|fw|aw)\s*:\s*", re.IGNORECASE)

def norm_subject(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    while _RE_PREFIX.match(s):               # peel stacked "Re: Fwd:" prefixes
        s = _RE_PREFIX.sub("", s, count=1)
    s = re.sub(r"\S+@\S+", " ", s)            # emails
    s = re.sub(r"\d[\d,./:\-]*", " <num> ", s) # ids / dates / amounts -> slot
    s = re.sub(r"[^a-z<>\s]", " ", s)          # punctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_body(s: str) -> str:
    s = clean_email(s)                        # reuse: strips quotes/sig/disclaimer/urls/emails
    s = s.lower()
    s = re.sub(r"\d[\d,./:\-]*", " <num> ", s)
    s = re.sub(r"[^a-z<>\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

pdf["subject_norm"] = pdf[SUBJECT_COL].map(norm_subject)
pdf["body_norm"]    = pdf[TEXT_COL].map(norm_body)

subj_counts = pdf["subject_norm"].value_counts()
body_counts = pdf["body_norm"].value_counts()
pdf["subject_count"]       = pdf["subject_norm"].map(subj_counts).astype("int64")
pdf["body_template_count"] = pdf["body_norm"].map(body_counts).astype("int64")
pdf["subject_is_common"]   = pdf["subject_count"]       >= COMMON_SUBJECT_MIN
pdf["body_is_common"]      = pdf["body_template_count"] >= COMMON_BODY_MIN

n_subj_tpl = int((subj_counts >= COMMON_SUBJECT_MIN).sum())
n_body_tpl = int((body_counts >= COMMON_BODY_MIN).sum())
print(f"{n_subj_tpl} recurring subject templates (>= {COMMON_SUBJECT_MIN}x) "
      f"covering {int(pdf['subject_is_common'].sum()):,} / {len(pdf):,} emails")
print(f"{n_body_tpl} recurring body templates (>= {COMMON_BODY_MIN}x) "
      f"covering {int(pdf['body_is_common'].sum()):,} / {len(pdf):,} emails")

print("\nMost common SUBJECTS (verbatim sample of each template):")
for sn in subj_counts.head(TOP_N_PRINT).index:
    ex = pdf.loc[pdf["subject_norm"] == sn, SUBJECT_COL].iloc[0]
    print(f"  {subj_counts[sn]:4d}x  {ex[:90]}")

print("\nMost common MESSAGES (verbatim sample of each template):")
for bn in body_counts.head(TOP_N_PRINT).index:
    ex = clean_email(pdf.loc[pdf["body_norm"] == bn, TEXT_COL].iloc[0])
    print(f"  {body_counts[bn]:4d}x  {ex[:110]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Embed


# CELL ********************

from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer(EMBEDDING_MODEL)
embeddings = embedder.encode(docs, show_progress_bar=True, batch_size=64)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Discover requirements — spherical K-Means, K by silhouette
# Embeddings are L2-normalized so Euclidean K-Means behaves as cosine ("spherical" K-Means).
# We sweep K over `[K_MIN, K_MAX]` and keep the best silhouette. Every email is assigned to one
# requirement — no outlier bucket. (Alternative: `AgglomerativeClustering(distance_threshold=...)`
# to let the data choose K directly.)


# CELL ********************

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

X  = np.asarray(embeddings, dtype="float32")
Xn = X / (np.linalg.norm(X, axis=1, keepdims=True) + 1e-9)     # spherical -> cosine

best = None
print("silhouette sweep (higher = tighter, better-separated):")
for k in range(K_MIN, min(K_MAX, len(Xn) - 1) + 1):
    km  = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
    lbl = km.fit_predict(Xn)
    sil = silhouette_score(Xn, lbl, metric="cosine")
    print(f"  k={k:2d}  silhouette={sil:.3f}")
    if best is None or sil > best[0]:
        best = (sil, k, km, lbl)

sil, K, km, labels = best
labels    = labels.astype(int)
centroids = km.cluster_centers_
cn = centroids / (np.linalg.norm(centroids, axis=1, keepdims=True) + 1e-9)
pdf["topic"] = labels
print(f"\nChosen K = {K}  (silhouette = {sil:.3f}); every email assigned, no outliers.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Classify + score confidence + validate separability
# **Nearest-centroid** is the deployable classifier: an email's requirement is the closest
# centroid (cosine). We record `confidence` and a top1−top2 `margin`; emails below the floors
# are flagged `needs_review` rather than trusted blindly. The **logistic-regression** block is a
# sanity check — high held-out accuracy means the requirements are cleanly separable in
# embedding space (it does **not** prove they are the *right* requirements).


# CELL ********************

from collections import Counter
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, classification_report

sims_all = Xn @ cn.T
assigned = sims_all[np.arange(len(Xn)), labels]
srt      = np.sort(sims_all, axis=1)
margin   = srt[:, -1] - srt[:, -2]
pdf["confidence"]   = assigned.astype(float)
pdf["margin"]       = margin.astype(float)
pdf["needs_review"] = (assigned < CONF_MIN) | (margin < MARGIN_MIN)
print(f"{int(pdf['needs_review'].sum()):,} / {len(pdf):,} emails below floors -> needs_review")

min_class = min(Counter(labels).values())
strat = labels if min_class >= 2 else None
Xtr, Xte, ytr, yte = train_test_split(X, labels, test_size=TEST_SIZE,
                                      random_state=RANDOM_STATE, stratify=strat)
clf = LogisticRegression(max_iter=1000, C=10.0)
clf.fit(Xtr, ytr)
pred = clf.predict(Xte)
print(f"\nHeld-out separability  accuracy={accuracy_score(yte, pred):.3f}  "
      f"macroF1={f1_score(yte, pred, average='macro'):.3f}")
print(classification_report(yte, pred, zero_division=0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. INSPECT — read the requirement, decide if it is real
# For each requirement: size, the recurring subjects, and the **medoid** email (most central
# real message). This is the only true validation step — name the real requirements and spot
# artifacts (a surviving footer, a template you failed to strip).


# CELL ********************

def medoid_idx(tid):
    pos = np.where(labels == tid)[0]
    s = Xn[pos] @ cn[tid]
    return int(pos[int(s.argmax())])

for tid in range(K):
    g = pdf[pdf["topic"] == tid]
    print(f"\n=== REQUIREMENT {tid}  (n={len(g)}, avg_conf={g['confidence'].mean():.2f}) ===")
    top_subj = g[g[SUBJECT_COL].str.strip() != ""][SUBJECT_COL].value_counts().head(3)
    print("recurring subjects:")
    for subj, cnt in top_subj.items():
        print(f"    {cnt:3d}x  {subj[:80]}")
    print("representative request (medoid):")
    print("    ", clean_email(pdf[TEXT_COL].iloc[medoid_idx(tid)])[:240].replace("\n", " "))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Build the two silver frames — assignments + summary
# Built **before the writer cell** (the writer consumes `assign_df`/`summary_df`). All columns
# scalar. Each requirement is described by a plain-language `request_summary` (its medoid) plus
# recurring subjects — no keywords. A reusable `classify_requests(subjects, bodies)` scores
# future emails with the same model.


# CELL ********************

import pandas as pd

def _mode_subject(g):
    s = g[g[SUBJECT_COL].str.strip() != ""][SUBJECT_COL]
    return s.value_counts().idxmax() if len(s) else ""
def _top_subjects(g, n=TOP_N_SUBJECTS):
    vc = g[g[SUBJECT_COL].str.strip() != ""][SUBJECT_COL].value_counts().head(n)
    return " | ".join(f"{cnt}x {subj[:70]}" for subj, cnt in vc.items())
def _example_message(g):
    bn  = g["body_norm"].value_counts().idxmax()
    raw = g.loc[g["body_norm"] == bn, TEXT_COL].iloc[0]
    return clean_email(raw)[:EXAMPLE_CHARS]
def _trim(text, n):
    text = clean_email(text)
    if len(text) <= n: return text
    cut = text[:n]; dot = cut.rfind(". ")
    return (cut[:dot + 1] if dot > 40 else cut).strip()

label_map, summary_map, topsubj_map, share_map, example_map, commoncnt_map, conf_map = ({} for _ in range(7))
for tid, g in pdf.groupby("topic"):
    label_map[tid]     = _mode_subject(g) or f"requirement {tid}"
    summary_map[tid]   = _trim(pdf[TEXT_COL].iloc[medoid_idx(tid)], SUMMARY_CHARS)
    topsubj_map[tid]   = _top_subjects(g)
    example_map[tid]   = _example_message(g)
    commoncnt_map[tid] = int(g["subject_is_common"].sum())
    conf_map[tid]      = float(g["confidence"].mean())
    dom = g[SUBJECT_COL].value_counts()
    share_map[tid]     = float(dom.iloc[0] / len(g)) if len(dom) else 0.0

def classify_requests(subjects, bodies):
    """Score NEW emails with the same embedder + discovered centroids."""
    comp = [((s + ". ") if isinstance(s, str) and s else "") + clean_email(b)
            for s, b in zip(subjects, bodies)]
    e  = embedder.encode(comp)
    en = e / (np.linalg.norm(e, axis=1, keepdims=True) + 1e-9)
    sims = en @ cn.T
    lab, conf = sims.argmax(1), sims.max(1)
    return pd.DataFrame({"topic": lab.astype(int),
                         "topic_label": [label_map.get(int(l), "") for l in lab],
                         "request_summary": [summary_map.get(int(l), "") for l in lab],
                         "confidence": conf.astype(float),
                         "needs_review": conf < CONF_MIN})

print("demo classify_requests on 3 existing emails:")
print(classify_requests(pdf[SUBJECT_COL].head(3).tolist(), pdf[TEXT_COL].head(3).tolist()))

pdf["topic_label"] = pdf["topic"].map(label_map)
assign_df = pdf[[ID_COL, SUBJECT_COL, TEXT_COL, "subject_norm", "subject_count", "subject_is_common",
                 "body_template_count", "body_is_common",
                 "topic", "topic_label", "confidence", "needs_review"]].copy()
assign_df = assign_df.rename(columns={SUBJECT_COL: "subject", TEXT_COL: "body"})
for c in ["subject", "subject_norm", "topic_label", "body"]:
    assign_df[c] = assign_df[c].fillna("").astype(str)
assign_df["topic"]               = assign_df["topic"].astype("int64")
assign_df["subject_count"]       = assign_df["subject_count"].astype("int64")
assign_df["body_template_count"] = assign_df["body_template_count"].astype("int64")
assign_df["confidence"]          = assign_df["confidence"].astype("float64")
assign_df["subject_is_common"]   = assign_df["subject_is_common"].astype(bool)
assign_df["body_is_common"]      = assign_df["body_is_common"].astype(bool)
assign_df["needs_review"]        = assign_df["needs_review"].astype(bool)

summary_df = (pdf["topic"].value_counts().sort_index()
              .rename_axis("topic").reset_index(name="email_count"))
summary_df["topic_label"]            = summary_df["topic"].map(label_map)
summary_df["request_summary"]        = summary_df["topic"].map(summary_map)
summary_df["top_subjects"]           = summary_df["topic"].map(topsubj_map)
summary_df["dominant_subject_share"] = summary_df["topic"].map(share_map)
summary_df["common_subject_emails"]  = summary_df["topic"].map(commoncnt_map)
summary_df["avg_confidence"]         = summary_df["topic"].map(conf_map)
summary_df["example_message"]        = summary_df["topic"].map(example_map)
for c in ["topic_label", "request_summary", "top_subjects", "example_message"]:
    summary_df[c] = summary_df[c].fillna("").astype(str)
summary_df["topic"]                  = summary_df["topic"].astype("int64")
summary_df["email_count"]            = summary_df["email_count"].astype("int64")
summary_df["common_subject_emails"]  = summary_df["common_subject_emails"].fillna(0).astype("int64")
summary_df["dominant_subject_share"] = summary_df["dominant_subject_share"].fillna(0.0).astype("float64")
summary_df["avg_confidence"]         = summary_df["avg_confidence"].fillna(0.0).astype("float64")
summary_df = summary_df.sort_values("email_count", ascending=False).reset_index(drop=True)

print(f"\nassign_df : {len(assign_df):,} rows x {assign_df.shape[1]} cols")
print(f"summary_df: {len(summary_df):,} rows x {summary_df.shape[1]} cols")
summary_df[["topic", "email_count", "topic_label", "request_summary", "avg_confidence"]].head(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Write to the SILVER lakehouse — single writer
# The cell below is the **only** writer. It uses the three-part
# `saveAsTable("lh_silver_banking_data.dbo.<table>")` name, which registers real lakehouse
# tables even though silver is not the default lakehouse.


# CELL ********************

# Try this way it should work... your way does not create a lakehouse table and since silver lakehouse is not the defualt it will create a new schema (email_topic_assignments) for example. 
# And save does not save as a table when the lakehouse is not the default

assign_spark_df = spark.createDataFrame(assign_df)
summary_spark_df = spark.createDataFrame(summary_df)

(assign_spark_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_silver_banking_data.dbo.email_topic_assignments"))

(summary_spark_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_silver_banking_data.dbo.email_topic_summary"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 11. Verify the write — read-back only (no writes)


# CELL ********************

for t in (ASSIGN_TABLE, SUMMARY_TABLE):
    df = spark.table(f"lh_silver_banking_data.dbo.{t}")
    print(f"\n{t}: {df.count():,} rows")
    df.printSchema()

spark.table(f"lh_silver_banking_data.dbo.{SUMMARY_TABLE}").show(10, truncate=80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Validation checklist — how not to fool yourself
# - **Read the summaries (the real test):** does each `request_summary` + recurring subjects
#   describe one coherent ask? Metrics cannot tell you this — only you can.
# - **K sanity:** if two requirements read identically, K is too high; if one summary blends two
#   asks, K is too low. Re-run the sweep over a narrower range.
# - **Confidence tail:** scan `needs_review` emails. A large tail means the corpus has asks the
#   current K does not cover — raise `K_MAX` and re-sweep, or treat them as a residual bucket.
# - **Separability vs correctness:** high LR accuracy means clusters are *separable*, not
#   *correct*. Do not mistake one for the other.
# - **Template cross-check:** compare cluster requirements against the 4b body/subject templates.
#   A frequent template with no matching requirement means discovery missed it.
# - **Stability:** change `RANDOM_STATE`; requirements that survive are real. The template layer
#   is deterministic and unaffected.

