# Microplate readings API

FastAPI + SQLite service that accepts whole-plate microplate readings as ordered
revisions and answers, per sample, with the newest reading that is actually
usable by the analysis pipeline.

Instruments that emit one plate row at a time (with possibly reordered
retransmits after a reconnect) use the **staging API**: the rows are buffered
until the plate is explicitly sealed, so analysis still only ever observes
complete plates.

* Interactive docs: <http://localhost:8000/docs> (register, send rows, seal and
  query samples all from there)
* OpenAPI schema: `http://localhost:8000/openapi.json`
* Health: `GET /health`

## Rules implemented

**Submission** — `POST /plates/{plate_id}/revisions`

* A plate is submitted as a whole (1–4 rows) with an increasing revision number
  and a UTC measurement time (`tested_at`).
* Every row has exactly one blank well, exactly one positive-control well and
  1–4 sample wells.
* Well positions and sample ids must be unique across the whole plate.
* Readings must be finite and non-negative (`NaN`, `Infinity`, `-1` → 422).
* Revisions strictly increase per plate. The first is `1`; a later submission
  must carry a higher number than the stored one (gaps are allowed, e.g. after a
  rejected payload). Omitting `revision` assigns the next number. A stale or
  duplicate revision is `409 revision_conflict` and nothing is written.

**Staging (row-by-row) API**

1. `POST /plates/{plate_id}/staging` registers a plate with its
   `target_revision`, UTC `tested_at` and `row_count` (1–4). Only one *open*
   staging record may exist per plate (`409 staging_active`); a sealed one stays
   queryable and does not block a new registration.
2. `PUT /plates/{plate_id}/staging/rows/{row_number}` submits one row in the
   existing row format (`row_number` is 1-based and must fit the declared
   `row_count`). Rows may arrive in any order.
   * First submission of a row: omit `expected_version`; the stored row starts
     at **version 1**.
   * Identical retransmit (same plate, same row, same content): the original
     stored row — same version, same `content_hash`, same timestamp — is
     returned; nothing is written again.
   * Correction (different content): must carry `expected_version` equal to the
     version currently stored. A missing or stale version is
     `409 row_version_conflict`; an accepted correction increments the version.
3. `POST /plates/{plate_id}/staging/seal` seals the plate. Sealing succeeds only
   when every declared row is present **and** the plate passes the existing
   plate-wide uniqueness and reading validation; the complete plate is then
   inserted exactly once as `target_revision` in a single transaction.
   * Missing rows → `409 staging_incomplete`; validation failure →
     `409 plate_validation_failed`. In both cases the staging record is kept so
     the bad rows can be corrected.
   * If the target revision is no longer ahead of the plate's current revision
     (e.g. a whole-plate submission won a race) → `409 revision_conflict`; the
     current sample readings are not changed and the staging is retained.
   * Sealing the same staging again replays the original receipt (`200` instead
     of `201`) and never creates another revision.
4. `GET /plates/{plate_id}/staging` reports the staging state: received and
   missing row numbers, per-row versions, and the sealed revision.

Staged rows are invisible to analysis until a successful seal: sample lookups
and plate history only contain complete, committed revisions. All staging state
lives in the SQLite file, so open and sealed staging records survive restarts.

**Quality gate** — a row qualifies only if `blank < 0.1` **and**
`0.9 <= positive_control <= 1.1`. Unqualified rows are stored and reported
(`qualified: false`) but never served.

**Lookup** — `GET /samples/{sample_id}/reading`

* Returns the qualified reading with the newest `tested_at`, together with its
  well, plate id, revision and timestamp.
* A newer plate whose row failed quality control does **not** displace an older
  qualified reading — the service falls back to the older qualified value.
* If two different plates are both qualified for that sample at exactly the
  newest `tested_at`, the answer is ambiguous: `409 ambiguous_reading` with all
  candidates, and no reading is served.
* The latest revision of a plate is authoritative, so correcting a plate
  immediately re-judges every sample on it (a correction can withdraw a reading
  or restore it).
* Unknown sample → `404 sample_not_found`; known sample with no usable reading →
  `404 no_qualified_reading`.

**Concurrency and consistency**

* Every revision (whole-plate submit or successful seal) is written inside one
  SQLite transaction: concurrent readers see either the previous plate or the
  complete new one, never a half plate.
* Row writes, seals and whole-plate submissions are serialised per process
  (plus `BEGIN IMMEDIATE` across processes) and adjudicated on the committed
  state: racing writers cannot produce duplicate revisions, partial plates or
  lost rows.
* A rejected or failed submission leaves the prior answers untouched.
* State lives in the SQLite file (`DATABASE_PATH`), so restarting the service
  reaches exactly the same conclusions, staging records remain queryable and
  revisions continue from where they stopped.

## Run with Docker Compose

```sh
docker compose up --build
```

The API listens on port 8000; the database (revisions **and** staging state) is
kept in the `plate-data` volume. Data survives `docker compose down` (use
`down -v` to wipe it deliberately).

## Run locally

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

`DATABASE_PATH` selects the SQLite file (default `data/plates.db`).

## Tests

```sh
pip install pytest httpx
python3 -m pytest tests -q
```

## Example

### Whole plate

```sh
curl -s -X POST localhost:8000/plates/P-001/revisions \
  -H 'content-type: application/json' \
  -d '{
        "plate_id": "P-001",
        "revision": 1,
        "tested_at": "2024-05-01T09:30:00Z",
        "rows": [
          {"blank":           {"well": "A1", "reading": 0.04},
           "positive_control":{"well": "A2", "reading": 1.02},
           "samples": [
             {"well": "A3", "sample_id": "S-1", "reading": 0.42},
             {"well": "A4", "sample_id": "S-2", "reading": 0.31}
           ]}
        ]
      }'

curl -s localhost:8000/samples/S-1/reading
```

```json
{
  "sample_id": "S-1",
  "reading": 0.42,
  "well": "A3",
  "plate_id": "P-001",
  "revision": 1,
  "tested_at": "2024-05-01T09:30:00Z"
}
```

### Row-by-row staging

```sh
# 1. register: plate P-002 will arrive as two rows and become revision 1
curl -s -X POST localhost:8000/plates/P-002/staging \
  -H 'content-type: application/json' \
  -d '{"target_revision": 1,
       "tested_at": "2024-05-01T10:00:00Z",
       "row_count": 2}'

# 2. rows may arrive out of order; the first PUT of each row returns version 1
curl -s -X PUT localhost:8000/plates/P-002/staging/rows/2 \
  -H 'content-type: application/json' \
  -d '{"row": {"blank":            {"well": "B1", "reading": 0.03},
               "positive_control": {"well": "B2", "reading": 1.05},
               "samples": [{"well": "B3", "sample_id": "S-4",
                            "reading": 0.41}]}}'

curl -s -X PUT localhost:8000/plates/P-002/staging/rows/1 \
  -H 'content-type: application/json' \
  -d '{"row": {"blank":            {"well": "A1", "reading": 0.04},
               "positive_control": {"well": "A2", "reading": 1.02},
               "samples": [{"well": "A3", "sample_id": "S-3",
                            "reading": 0.42}]}}'

# identical retransmit -> original row/version; correction needs expected_version
curl -s -X PUT localhost:8000/plates/P-002/staging/rows/1 \
  -H 'content-type: application/json' \
  -d '{"expected_version": 1,
       "row": {"blank":            {"well": "A1", "reading": 0.05},
               "positive_control": {"well": "A2", "reading": 1.02},
               "samples": [{"well": "A3", "sample_id": "S-3",
                            "reading": 0.42}]}}'

# 3. inspect progress, then seal
curl -s localhost:8000/plates/P-002/staging
curl -s -X POST localhost:8000/plates/P-002/staging/seal
```

`GET /plates/{plate_id}` returns the stored revision history for auditing.

