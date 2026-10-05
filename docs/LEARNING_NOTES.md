# Learning Notes — What We Built and Why

This is a plain-language explanation of every concept behind this project, in
the order we actually built it. Read this once end to end, then come back to
sections as reference while you work. If a term is used before it's
explained, it gets explained in the section it's introduced.

---

## 1. The big picture

Before any tools: what is this project actually doing?

Imagine a company gets daily data files from different sources (say, two
different hospital billing systems), and those files don't use the same
column names. Someone needs to:

1. Collect all these files in one place
2. Make their columns consistent with each other
3. Combine them into one clean dataset
4. Only process *new* data each day, not re-process everything from scratch
5. Store the clean result somewhere fast to query
6. Let a reporting tool (like Power BI) read from it

That's the whole project. Everything below is just "what Azure tool handles
which of these 6 steps, and why that tool."

---

## 2. Azure account structure

- **Subscription** — your billing boundary. Everything you create belongs to
  one subscription. We used "Azure subscription 1," created from your free
  account.
- **Resource group** (`rg-de-portfolio`) — a folder for related resources.
  We put everything in one resource group so it's easy to find, manage, and
  (eventually) delete all at once if needed. It has no effect on how
  resources work — it's pure organization.
- **Region** (`centralindia`) — the physical datacenter location. We picked
  one close to you for lower latency; in a real job this is usually dictated
  by company policy/compliance, not personal preference.

---

## 3. ADLS Gen2 — where the data physically lives

**ADLS Gen2** (Azure Data Lake Storage Gen2) is Azure's storage system for
data engineering. It's built on top of regular Azure Blob Storage, with one
key feature switched on: **hierarchical namespace (HNS)**.

- Without HNS (plain Blob Storage): storage is just a flat bucket of files
  with long names that *look* like folders (`raw/2026/09/file.csv`) but
  aren't really folders underneath.
- With HNS (ADLS Gen2): there are real folders, which makes operations like
  "rename this folder" or "list everything under this path" fast and
  efficient — something Spark relies on constantly when reading data.

That's why, when we created the storage account, we passed `--hns true`, and
why the verification showed `IsHnsEnabled: True` — that's the setting that
makes it "Gen2" instead of plain Blob storage.

### Containers

A **container** is the top-level folder inside a storage account (similar to
a bucket in S3, if you've seen AWS). We made three:

- **raw** — exactly what it sounds like: source files land here, untouched.
- **curated** — the cleaned, transformed, analytics-ready output (Parquet
  files) goes here.
- **control** — a small folder holding operational metadata about the
  pipeline itself — specifically, the watermark file (explained in section 7).

Splitting raw vs. curated is a very standard pattern (sometimes called
"medallion architecture" — raw/bronze, cleaned/silver, aggregated/gold).
It exists so you always keep the original, untouched source data in case you
ever need to re-process it with fixed logic, and so consumers (BI tools)
only ever touch the clean "curated" zone.

---

## 4. Azure Databricks — where the data gets transformed

**Databricks** is a managed platform for running **Apache Spark**, a
distributed data-processing engine. "Distributed" means it can split work
across many machines — the whole point is handling data too large for one
computer/one pandas DataFrame to process in memory.

- **Workspace** (`dbw-de-portfolio`) — the Databricks environment itself; a
  website (`adb-....azuredatabricks.net`) where you write and run code.
- **Cluster** — the actual compute (virtual machines) that runs your code.
  A workspace is just the UI/control plane; nothing executes until a cluster
  is running. This is why cluster time is billed and workspace creation
  isn't — the workspace is just the empty shell, the cluster is the engine.
  This is also why we set "terminate after 30 min of inactivity" — so you
  don't pay for a cluster sitting idle.
- **Notebook** — a file of code cells you run interactively, attached to a
  cluster. This is where you actually write the PySpark logic.

### Why PySpark instead of plain pandas?

Pandas loads an entire dataset into memory on one machine — great for small
files (which is why we still use it for the small Excel files), bad for
large-scale data. **PySpark** is the Python API for Spark: it looks similar
to pandas (DataFrames, `.filter()`, `.groupBy()`, etc.) but the actual work
happens across a cluster, and it's lazy — operations don't run immediately,
they build up a plan that only executes when you ask for a result (like
`.count()` or `.show()`). That's why your resume lists PySpark specifically,
not just Python — it's the tool suited to the data *volume* a real pipeline
handles, even though the code looks deceptively similar to pandas.

---

## 5. Unity Catalog — how Databricks is allowed to touch your storage

This was the trickiest part conceptually, so slow down here.

By default, Databricks and your ADLS Gen2 storage account are two completely
separate, unrelated Azure resources. Nothing lets Databricks read from or
write to your storage automatically — you have to explicitly grant it
permission. There are two common ways:

**Option A — a secret (what we avoided):** create a "service principal"
(an identity for an application, not a person) with a password-like secret,
and paste that secret into notebook code. This works, but that secret is a
piece of text that could leak (committed to git by accident, printed in a
log, etc.) and has to be manually rotated. It's the older, more common
pattern you'll see in a lot of existing tutorials/production code, but
auto-flagged by your own tooling as risky, and for good reason.

**Option B — a managed identity (what we did):** Azure can give a resource
its own identity *without any password/secret ever existing*, similar to
how your own Azure login proves who you are without you typing a database
password. We created an **Access Connector** (`ac-de-portfolio`), which is
exactly this: an identity object with no secret attached. Then we granted
*that identity* permission on the storage account — the same way you'd give
a coworker a specific door key instead of handing them your master key.

**The permission itself (RBAC role):** `Storage Blob Data Contributor` is
one of Azure's built-in roles — it means "can read, write, and delete
data inside containers," but does *not* mean "can delete the whole storage
account" or "can change its settings." This is the principle of **least
privilege**: grant exactly the access needed, nothing more. This is also why
we had to separately grant *your own account* the same role — being the
subscription's owner lets you manage/configure resources (the "control
plane"), but doesn't automatically grant you permission to read/write the
actual data inside them (the "data plane"). These are deliberately separate
in Azure's permission model.

**Unity Catalog** itself is Databricks' governance layer that ties these
pieces together:
- **Storage credential** — "here's an identity (our Access Connector) that's
  allowed to authenticate to Azure storage."
- **External location** — "here's a specific storage path (e.g., the `raw`
  container), and here's which storage credential to use when touching it."

Once both exist, any notebook on the workspace can read/write
`abfss://raw@yourstorageaccount.dfs.core.windows.net/...` and Unity Catalog
transparently handles authentication behind the scenes — no secret in your
code at all.

(The `abfss://` prefix, by the way, stands for "Azure Blob File System
Secure" — it's just the URL scheme Spark/Hadoop use to address ADLS Gen2
paths, the same way `https://` addresses a website.)

### "File events" (the thing that showed Failed)

This is a separate, optional feature: Databricks can subscribe to Azure
Event Grid notifications so that when a new file lands in storage, it knows
immediately instead of having to repeatedly list the folder to check. It
needs three more permissions beyond basic read/write (to create a queue,
subscribe to events, etc.). We added those roles for completeness, but our
pipeline doesn't use this feature (it's relevant for Databricks' "Auto
Loader" tool, which we're not using) — it's fine that it wasn't required to
get the "Success" results for Read/Write/List/Delete, which are what
actually matter for us.

---

## 6. Why two different errors happened (and why they're not failures)

### Error 1: `IllegalAccessException ... non /Workspace local filesystem path`

Our **cluster's access mode** is "Shared" — meaning multiple users could
attach to the same cluster, so Databricks isolates each user's code from the
machine's actual local disk, for security (so User A's notebook can't read
User B's leftover temp files, for instance). Our first script tried to copy
a file from ADLS onto the cluster's local `/tmp` folder with
`dbutils.fs.cp`, which Shared clusters block.

**Fix:** never touch local disk at all. Instead, read the file as raw bytes
directly through Spark (`spark.read.format("binaryFile")`), which is allowed
because it goes through Unity Catalog's credential system, not the local
filesystem. Then hand those bytes to pandas *in memory* using `io.BytesIO`
— pandas thinks it's reading a normal file, but no disk is involved.

### Error 2: `Missing optional dependency 'openpyxl'`

`openpyxl` is the library pandas uses under the hood to actually parse
`.xlsx` files. On your laptop, we `pip install`-ed it ourselves, so it was
there. A brand-new Databricks cluster is a fresh virtual machine — it has
core libraries (PySpark, pandas) pre-installed, but not every optional one.
`%pip install openpyxl` installs it just for that cluster's Python
environment, and `dbutils.library.restartPython()` restarts the Python
process so the newly installed package is actually loaded (Python caches
what's importable at process start).

Neither of these were mistakes on your part — they're the exact kind of
environment quirks every data engineer hits constantly when moving code from
"works on my laptop" to "works on a managed cluster." Knowing *why* they
happen (isolation security, fresh environment) is the actual skill; the fix
itself is secondary.

---

## 7. The transform logic itself

### Schema normalization

Source A uses columns `ClaimID, ClaimDate, Amount, Status`. Source B uses
`claim_id, claim_date, amount_usd, status` — different names, same meaning.
Before you can combine them, every source must be renamed to one shared
("canonical") schema: `claim_id, claim_date, amount, status`. This is what
the `SCHEMA_MAP` dictionary in the script does — it's a translation table
per source file.

### Union

Once every source has identical column names and order, Spark's
`unionByName()` stacks them into one DataFrame — literally just "append
these rows together." This only works cleanly *because* we normalized the
schema first; unioning mismatched schemas either errors or silently
produces garbage.

### Watermark-based incremental load

This is the core ETL concept behind "incremental load" on your resume.

The problem: every time the pipeline runs, re-processing *all* historical
data (even data from six months ago that never changes) wastes time and
compute cost. The fix: remember the most recent data timestamp you already
successfully processed — the **watermark** — and next run, only process
rows newer than that.

We store the watermark as a tiny JSON file (`{"last_loaded_date": "..."}`)
in the `control` container. Each run:
1. Read the stored watermark (or treat it as "none" on the very first run —
   a **full load**)
2. Filter the incoming data to only rows where `claim_date >` that watermark
3. Process only those rows
4. After a successful write, update the watermark to the newest date just
   processed

This is exactly why running the script twice showed different output: first
run had no watermark (full load, 90 records), second run had a watermark
from the first run and correctly found 0 new records, since nothing newer
than `2026-09-21` existed yet.

### Parquet + partitioning

**Parquet** is a columnar file format (as opposed to row-based formats like
CSV). Columnar storage lets analytics queries ("give me the average amount
for Approved claims") read only the columns they need, rather than scanning
every field of every row — much faster for BI/reporting workloads, which is
why it's the standard output format for data lakes.

`partitionBy("status")` physically splits the output into separate folders
per status value (`status=Approved/`, `status=Pending/`, etc.). A query that
filters `WHERE status = 'Approved'` can then skip reading the other folders
entirely instead of scanning everything and filtering afterward — a free
performance win for a column you frequently filter on.

---

## 8. What's still ahead (and why each piece exists)

- **Azure Data Factory (ADF)** — right now, you manually click "run" in the
  notebook. ADF is the *orchestrator*: it triggers this notebook on a
  schedule (e.g., daily at 2 AM) with no human involved, and can send an
  alert if the run fails. This is what makes a pipeline "production," not
  just a script.
- **Synapse serverless SQL** — right now, the curated Parquet data can only
  be read by writing Spark/Python code. Synapse lets you register that
  Parquet folder as something queryable with plain SQL
  (`SELECT * FROM claims WHERE status = 'Approved'`), which is what BI tools
  and analysts actually expect to connect to.
- **Power BI** — the actual dashboard/report layer on top of that SQL
  endpoint, for non-technical stakeholders to view.

Each of these exists to answer one question: "how does a human or another
system actually *consume* what this pipeline produced, without needing to
know Python or Spark?"

---

## How to actually retain this

Don't just re-read this file. Next time we touch a new piece (ADF, Synapse),
try explaining *in your own words* — even just to yourself — what you think
it's for and why, before I confirm or correct it. Getting it wrong first and
then being corrected sticks better than being told the right answer upfront.
