# Query Breakdown: Elasticsearch vs PostgreSQL Full-Text Search (GIN)

This document explains how each benchmark query (1–6) is implemented in this repo for:

- **Elasticsearch**: JSON DSL executed via `/_search`.
- **PostgreSQL 18**: SQL using built-in full-text search (`tsvector` + GIN) executed via psycopg2.

Source of truth for query templates:
- Elasticsearch: `scripts/elasticsearch_benchmark.py`
- Postgres: `scripts/benchmark_postgres_fts.py`

Important: the benchmark is designed to be *similar* across systems (top-K search, phrase search, boolean/disjunction, and a parent/child workload), but the engines have different scoring models and different join mechanics, so the work is not perfectly identical.

---

## Common execution model differences

### Elasticsearch
- Uses a **Lucene inverted index** with segment-level execution.
- Typically ranks results with **BM25** and returns top-K by score.
- Query cost is influenced by:
  - segment/shard count and merge state
  - whether scoring is needed
  - whether total-hit tracking is enabled (`track_total_hits`)

### PostgreSQL full-text search (tsvector + GIN)
- Uses a **GIN index** on a `tsvector` column for fast candidate selection.
- Ranking is computed with `ts_rank_cd(...)` (not BM25).
- Query cost is influenced by:
  - GIN selectivity (term frequency / tsquery structure)
  - ranking/sorting work for top-K
  - join strategy for Query 6

Schema details (see `scripts/benchmark_postgres_fts.py`):
- `documents(content_tsv tsvector GENERATED ALWAYS AS ...) STORED` with `CREATE INDEX ... USING gin(content_tsv)`
- `child_documents(parent_id uuid, data jsonb)` with a btree index on `parent_id`

---

## Query 1 — Simple Search

### Intent
Single-term full-text query (e.g., `strategy`), return top 10 results.

### Elasticsearch
Uses `match` over `content`, sorts by `_score`, `size: 10`.

### PostgreSQL
Uses `plainto_tsquery` and ranks via `ts_rank_cd`:

```sql
WITH q AS (SELECT plainto_tsquery('english', $1) AS query)
SELECT id, title
FROM documents, q
WHERE documents.content_tsv @@ q.query
ORDER BY ts_rank_cd(documents.content_tsv, q.query) DESC
LIMIT 10;
```

---

## Query 2 — Phrase Search

### Intent
Exact phrase match (e.g., `project management`), return top 10.

### Elasticsearch
Uses `match_phrase` over `content`.

### PostgreSQL
Uses `phraseto_tsquery`:

```sql
WITH q AS (SELECT phraseto_tsquery('english', $1) AS query)
SELECT id, title
FROM documents, q
WHERE documents.content_tsv @@ q.query
ORDER BY ts_rank_cd(documents.content_tsv, q.query) DESC
LIMIT 10;
```

---

## Query 3 — Complex Query (OR / Disjunction)

### Intent
Two-term OR query, return top 20.

### Elasticsearch
Uses a `bool.should` with two `match` clauses.

### PostgreSQL
Uses `websearch_to_tsquery` to express `term1 OR term2`:

```sql
WITH q AS (SELECT websearch_to_tsquery('english', $1) AS query)
SELECT id, title
FROM documents, q
WHERE documents.content_tsv @@ q.query
ORDER BY ts_rank_cd(documents.content_tsv, q.query) DESC
LIMIT 20;
```

Where `$1` is a string like `"global OR initiative"`.

---

## Query 4 — Top-N Query

### Intent
Single-term search with a higher limit (N from config; default 50).

### PostgreSQL
Same as Query 1 but `LIMIT N`.

---

## Query 5 — Boolean Query (must + should + not)

### Intent
Mix required terms and prohibited terms.

### Elasticsearch
Uses `bool.must`, `bool.should`, and `bool.must_not`.

### PostgreSQL (as benchmarked)
This repo currently makes both the “must” and “should” terms required on the Postgres side (and excludes `not`):

```sql
WITH q AS (SELECT websearch_to_tsquery('english', $1) AS query)
SELECT id, title
FROM documents, q
WHERE documents.content_tsv @@ q.query
ORDER BY ts_rank_cd(documents.content_tsv, q.query) DESC
LIMIT 10;
```

Where `$1` is a string like `"strategy growth -risk"`.

Note: Elasticsearch `should` clauses are often optional unless `minimum_should_match` is set, so this query can still differ in semantics.

---

## Query 6 — JOIN Query (parents + children)

### Intent
Return parent docs matching a full-text filter and also return related child data.

### Elasticsearch
Uses parent/child join (`join_field`) with `has_child` and `inner_hits`.

### PostgreSQL
Uses a relational join on `child_documents.parent_id`:

```sql
WITH q AS (SELECT plainto_tsquery('english', $1) AS query)
SELECT d.id, d.title, c.data
FROM documents d
JOIN child_documents c ON c.parent_id = d.id, q
WHERE d.content_tsv @@ q.query
LIMIT 10;
```

---

## Notes for interpreting results

- **Scoring differs**: Elasticsearch uses BM25, Postgres uses `ts_rank_cd`, so ordering and cost can differ.
- **Join mechanics differ**: Elasticsearch parent/child join is not a relational join and can be expensive under concurrency; Postgres uses an indexed join on `parent_id`.
- **Top-K sorting matters**: any query that orders by score may spend more CPU than a pure filter+limit.
