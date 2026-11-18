"""SQLite persistence and plate-quality / sample-eligibility logic.

Design notes
------------
* Each submission for a plate is a new *revision*.  A revision is written in a
  single SQLite transaction, so readers never observe half a plate and a failed
  submission leaves every earlier revision untouched.
* WAL journalling plus one connection per operation (and a ``busy_timeout``)
  lets concurrent submissions and queries run without readers being blocked by
  the writer; a read transaction sees one committed snapshot.
* The database file is the only source of truth, so a restart (or a second
  process) reaches exactly the same conclusions.
* A plate's latest revision is the authoritative judgement of that plate: a
  correction re-judges every sample on the plate immediately, because queries
  are computed from committed revisions rather than from cached values.
* A sample is served from the newest ``tested_at`` among plates whose latest
  revision carries a *qualified* row for it.  Unqualified (failed) rows never
  compete, which is what lets a failed retest fall back to an older qualified
  reading.  If several distinct plates are qualified at exactly that newest
  instant the answer is ambiguous and no reading is served.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Optional, Sequence

# Quality thresholds from the specification.
BLANK_LIMIT = 0.1  # blank must be strictly below this
CONTROL_MIN = 0.9  # positive control must lie within [CONTROL_MIN, CONTROL_MAX]
CONTROL_MAX = 1.1

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS plates (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    plate_id     TEXT    NOT NULL,
    revision     INTEGER NOT NULL,
    tested_at    TEXT    NOT NULL,
    submitted_at TEXT    NOT NULL,
    UNIQUE (plate_id, revision)
);

CREATE TABLE IF NOT EXISTS rows (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    plate_pk     INTEGER NOT NULL REFERENCES plates(id) ON DELETE CASCADE,
    row_index    INTEGER NOT NULL,
    blank        REAL    NOT NULL,
    blank_well   TEXT    NOT NULL,
    control      REAL    NOT NULL,
    control_well TEXT    NOT NULL,
    qualified    INTEGER NOT NULL CHECK (qualified IN (0, 1)),
    UNIQUE (plate_pk, row_index)
);

CREATE TABLE IF NOT EXISTS samples (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    plate_pk  INTEGER NOT NULL REFERENCES plates(id) ON DELETE CASCADE,
    row_pk    INTEGER NOT NULL REFERENCES rows(id) ON DELETE CASCADE,
    sample_id TEXT    NOT NULL,
    well      TEXT    NOT NULL,
    reading   REAL    NOT NULL CHECK (reading >= 0),
    UNIQUE (plate_pk, sample_id),
    UNIQUE (plate_pk, well)
);

CREATE INDEX IF NOT EXISTS idx_plates_plate_rev ON plates(plate_id, revision DESC);
CREATE INDEX IF NOT EXISTS idx_samples_sample ON samples(sample_id);

-- ----------------------------------------------------------------------------
-- Staging (incremental) submissions.
--
-- An instrument may deliver a plate one row at a time, and retransmits after a
-- reconnect may arrive out of order.  A staging record owns the rows until the
-- plate is explicitly sealed; only a successful seal is promoted to a normal
-- revision in ``plates``, so analysis queries never see a half plate.  The
-- whole staging area lives in the same SQLite file, hence survives restarts.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging_plates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    plate_id        TEXT    NOT NULL,
    target_revision INTEGER NOT NULL,
    row_count       INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 4),
    tested_at       TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    sealed_at       TEXT,
    sealed_revision INTEGER
);

-- At most one *open* (unsealed) staging record per plate id.
CREATE UNIQUE INDEX IF NOT EXISTS idx_staging_open_per_plate
    ON staging_plates(plate_id) WHERE sealed_at IS NULL;

CREATE TABLE IF NOT EXISTS staging_rows (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    staging_pk       INTEGER NOT NULL
        REFERENCES staging_plates(id) ON DELETE CASCADE,
    row_number       INTEGER NOT NULL CHECK (row_number BETWEEN 1 AND 4),
    version          INTEGER NOT NULL CHECK (version >= 1),
    blank            REAL    NOT NULL,
    blank_well       TEXT    NOT NULL,
    control          REAL    NOT NULL,
    control_well     TEXT    NOT NULL,
    -- Canonical JSON of the row as submitted: the idempotency / optimistic
    -- concurrency key.  Identical retransmits hash to the same value.
    content_hash     TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL,
    UNIQUE (staging_pk, row_number)
);

CREATE TABLE IF NOT EXISTS staging_samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    staging_pk  INTEGER NOT NULL
        REFERENCES staging_plates(id) ON DELETE CASCADE,
    row_pk      INTEGER NOT NULL
        REFERENCES staging_rows(id) ON DELETE CASCADE,
    sample_id   TEXT    NOT NULL,
    well        TEXT    NOT NULL,
    reading     REAL    NOT NULL CHECK (reading >= 0)
);
"""


class RevisionConflict(Exception):
    """The submitted revision does not increase the plate's current revision."""

    def __init__(self, plate_id: str, next_revision: int, latest: int) -> None:
        self.plate_id = plate_id
        self.next_revision = next_revision
        self.latest = latest
        super().__init__(
            f"plate {plate_id!r} is at revision {latest}; "
            f"revision must be greater (next available: {next_revision})"
        )


class StagingError(Exception):
    """Base class for staging-area failures; nothing is written on raise."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class StagingNotFound(StagingError):
    def __init__(self, plate_id: str) -> None:
        super().__init__(
            "staging_not_found", f"plate {plate_id!r} has no staging record"
        )


class StagingActive(StagingError):
    """An unsealed staging record already exists for the plate."""

    def __init__(self, plate_id: str) -> None:
        super().__init__(
            "staging_active",
            f"plate {plate_id!r} already has an open staging record; seal or "
            "abandon it before registering another one",
        )


class StagingSealed(StagingError):
    """The staging record is already sealed; no more rows may be written."""

    def __init__(self, plate_id: str, sealed_revision: int) -> None:
        self.sealed_revision = sealed_revision
        super().__init__(
            "staging_sealed",
            f"staging for plate {plate_id!r} is sealed as revision "
            f"{sealed_revision}",
        )


class RowOutOfRange(StagingError):
    def __init__(self, plate_id: str, row_number: int, row_count: int) -> None:
        super().__init__(
            "row_out_of_range",
            f"row_number {row_number} is outside 1..{row_count} declared when "
            f"staging for plate {plate_id!r} was registered",
        )


class RowVersionConflict(StagingError):
    """A different row body arrived without the matching seen version."""

    def __init__(
        self,
        plate_id: str,
        row_number: int,
        current_version: int,
        expected_version: Optional[int],
    ) -> None:
        self.current_version = current_version
        self.expected_version = expected_version
        if expected_version is None:
            message = (
                f"row {row_number} of plate {plate_id!r} already exists at "
                f"version {current_version}; a correction must repeat the "
                "version currently stored as expected_version"
            )
        else:
            message = (
                f"row {row_number} of plate {plate_id!r} is at version "
                f"{current_version}, not {expected_version}; refetch the row "
                "and retry with the version you have seen"
            )
        super().__init__("row_version_conflict", message)


class StagingIncomplete(StagingError):
    """Seal requested while some declared rows are still missing."""

    def __init__(self, plate_id: str, missing: Sequence[int], have: int) -> None:
        self.missing = tuple(missing)
        self.have = have
        super().__init__(
            "staging_incomplete",
            f"plate {plate_id!r} is missing row(s) {list(self.missing)} "
            f"({have} of the declared rows present); nothing was sealed",
        )


class PlateValidationError(StagingError):
    """Plate-wide validation failed at seal time; staging is retained."""

    def __init__(self, plate_id: str, issues: Sequence[str]) -> None:
        self.issues = tuple(issues)
        super().__init__(
            "plate_validation_failed",
            f"plate {plate_id!r} failed plate validation: "
            + "; ".join(self.issues),
        )


@dataclass(frozen=True)
class SampleRow:
    sample_id: str
    well: str
    reading: float


@dataclass(frozen=True)
class SubmitRow:
    blank: float
    blank_well: str
    control: float
    control_well: str
    samples: Sequence[SampleRow]

    @property
    def qualified(self) -> bool:
        return self.blank < BLANK_LIMIT and CONTROL_MIN <= self.control <= CONTROL_MAX


@dataclass(frozen=True)
class QualifiedReading:
    sample_id: str
    reading: float
    well: str
    plate_id: str
    revision: int
    tested_at: str  # canonical UTC ISO-8601, e.g. 2024-01-01T00:00:00.000000Z


@dataclass(frozen=True)
class Conflict:
    sample_id: str
    tested_at: str
    candidates: Sequence[QualifiedReading]


@dataclass(frozen=True)
class LookupResult:
    sample_id: str
    reading: Optional[QualifiedReading]
    conflict: Optional[Conflict]

    @property
    def found(self) -> bool:
        return self.reading is not None


@dataclass(frozen=True)
class PlateRowOut:
    row_index: int
    blank: float
    blank_well: str
    control: float
    control_well: str
    qualified: bool
    samples: Sequence[SampleRow]


@dataclass(frozen=True)
class PlateRevision:
    revision: int
    tested_at: str
    submitted_at: str
    rows: Sequence[PlateRowOut]


@dataclass(frozen=True)
class SubmitResult:
    plate_id: str
    revision: int
    tested_at: str
    submitted_at: str
    rows: Sequence[PlateRowOut]

    @property
    def qualified_rows(self) -> int:
        return sum(1 for row in self.rows if row.qualified)

    @property
    def rejected_rows(self) -> int:
        return len(self.rows) - self.qualified_rows


@dataclass(frozen=True)
class StagingRowReceipt:
    row_number: int
    version: int
    content_hash: str
    row: PlateRowOut
    updated_at: str


@dataclass(frozen=True)
class StagingState:
    plate_id: str
    target_revision: int
    row_count: int
    tested_at: str
    created_at: str
    sealed: bool
    sealed_at: Optional[str] = None
    sealed_revision: Optional[int] = None
    rows: Sequence[StagingRowReceipt] = field(default_factory=tuple)

    @property
    def received_row_numbers(self) -> tuple[int, ...]:
        return tuple(r.row_number for r in self.rows)

    @property
    def missing_row_numbers(self) -> tuple[int, ...]:
        have = set(self.received_row_numbers)
        return tuple(n for n in range(1, self.row_count + 1) if n not in have)


def utc_iso(dt: datetime) -> str:
    """Canonicalise a datetime to fixed-width UTC ISO-8601.

    Fixed width means lexicographic order equals chronological order, which the
    SQL ordering and ``MAX(tested_at)`` rely on.
    """

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return (
        f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}T"
        f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{dt.microsecond:06d}Z"
    )


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def canonical_row_hash(row: SubmitRow) -> str:
    """Stable textual identity of a row's content.

    Two submissions carry "the same row" exactly when their canonical hashes
    match, independent of dict ordering or whitespace.  Readings are rendered
    through ``repr`` so e.g. ``0.30`` and ``0.3`` compare equal while genuinely
    different readings do not.
    """

    payload = {
        "blank_well": row.blank_well,
        "blank": _number_key(row.blank),
        "control_well": row.control_well,
        "control": _number_key(row.control),
        "samples": [
            {
                "sample_id": s.sample_id,
                "well": s.well,
                "reading": _number_key(s.reading),
            }
            for s in row.samples
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _number_key(value: float) -> float | str:
    # -0.0 and 0.0 are the same reading; everything else round-trips via repr.
    if value == 0:
        return 0.0
    return float(repr(value))


def validate_plate_rows(rows: Sequence[SubmitRow]) -> list[str]:
    """Plate-wide laboratory checks applied to a complete set of rows.

    Per-row shape rules (1-4 samples, finite non-negative readings) are already
    enforced at the schema layer.  This enforces uniqueness of well positions
    and sample ids across the whole plate and returns every problem found so
    the client can correct the staging rows in one round trip.
    """

    issues: list[str] = []
    seen_wells: dict[str, int] = {}
    seen_samples: dict[str, int] = {}
    for index, row in enumerate(rows):
        wells = [
            row.blank_well,
            row.control_well,
            *(s.well for s in row.samples),
        ]
        for well in wells:
            key = well.casefold()
            if key in seen_wells:
                issues.append(
                    f"well {well!r} appears more than once on the plate "
                    f"(rows {seen_wells[key]} and {index + 1})"
                )
            else:
                seen_wells[key] = index + 1
        for sample in row.samples:
            if sample.sample_id in seen_samples:
                issues.append(
                    f"sample_id {sample.sample_id!r} appears more than once on "
                    "the plate"
                )
            else:
                seen_samples[sample.sample_id] = index + 1
    return issues


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._write_lock = threading.Lock()
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA busy_timeout = 30000")
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ writes

    def submit_plate(
        self,
        plate_id: str,
        revision: Optional[int],
        tested_at: datetime,
        rows: Sequence[SubmitRow],
    ) -> SubmitResult:
        """Append one revision of a plate atomically.

        Revisions increase strictly per plate.  ``revision`` may be omitted, in
        which case the next free number is assigned.  A revision that does not
        exceed the plate's current one raises :class:`RevisionConflict` and
        nothing at all is written.
        """

        tested = utc_iso(tested_at)

        # Serialise writers inside this process so "read the current revision
        # then insert" is one critical section.  SQLite's BEGIN IMMEDIATE write
        # lock covers other processes; the transaction makes the whole plate
        # (all rows and wells) appear atomically on commit.
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    current = conn.execute(
                        "SELECT COALESCE(MAX(revision), 0) FROM plates WHERE plate_id = ?",
                        (plate_id,),
                    ).fetchone()[0]
                    if revision is None:
                        revision = current + 1
                    elif revision <= current:
                        raise RevisionConflict(plate_id, current + 1, current)

                    _plate_pk, now = self._insert_revision(
                        conn, plate_id, revision, tested, rows
                    )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")

        assert revision is not None
        out_rows = self._rows_to_out(rows)
        return SubmitResult(
            plate_id=plate_id,
            revision=revision,
            tested_at=tested,
            submitted_at=now,
            rows=out_rows,
        )

    @staticmethod
    def _insert_revision(
        conn: sqlite3.Connection,
        plate_id: str,
        revision: int,
        tested: str,
        rows: Sequence[SubmitRow],
    ) -> tuple[int, str]:
        """Insert one complete plate revision inside the caller's transaction."""

        now = utc_iso(datetime.now(timezone.utc))
        cur = conn.execute(
            "INSERT INTO plates (plate_id, revision, tested_at, submitted_at)"
            " VALUES (?, ?, ?, ?)",
            (plate_id, revision, tested, now),
        )
        plate_pk = cur.lastrowid

        for index, row in enumerate(rows):
            cur = conn.execute(
                "INSERT INTO rows (plate_pk, row_index, blank, blank_well,"
                " control, control_well, qualified)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    plate_pk,
                    index,
                    row.blank,
                    row.blank_well,
                    row.control,
                    row.control_well,
                    int(row.qualified),
                ),
            )
            row_pk = cur.lastrowid
            conn.executemany(
                "INSERT INTO samples (plate_pk, row_pk, sample_id, well, reading)"
                " VALUES (?, ?, ?, ?, ?)",
                [
                    (plate_pk, row_pk, s.sample_id, s.well, s.reading)
                    for s in row.samples
                ],
            )
        return plate_pk, now

    @staticmethod
    def _rows_to_out(rows: Sequence[SubmitRow]) -> tuple[PlateRowOut, ...]:
        return tuple(
            PlateRowOut(
                row_index=index,
                blank=row.blank,
                blank_well=row.blank_well,
                control=row.control,
                control_well=row.control_well,
                qualified=row.qualified,
                samples=tuple(row.samples),
            )
            for index, row in enumerate(rows)
        )

    # --------------------------------------------------------------- staging

    def register_staging(
        self,
        plate_id: str,
        target_revision: int,
        tested_at: datetime,
        row_count: int,
    ) -> StagingState:
        """Open a staging record that will collect ``row_count`` rows.

        Only one *open* staging record may exist per plate; a sealed one stays
        around for auditing and does not block a new registration.
        """

        tested = utc_iso(tested_at)
        now = utc_iso(datetime.now(timezone.utc))
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    open_row = conn.execute(
                        "SELECT id FROM staging_plates"
                        " WHERE plate_id = ? AND sealed_at IS NULL",
                        (plate_id,),
                    ).fetchone()
                    if open_row is not None:
                        raise StagingActive(plate_id)
                    conn.execute(
                        "INSERT INTO staging_plates (plate_id, target_revision,"
                        " row_count, tested_at, created_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (plate_id, target_revision, row_count, tested, now),
                    )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")

        return StagingState(
            plate_id=plate_id,
            target_revision=target_revision,
            row_count=row_count,
            tested_at=tested,
            created_at=now,
            sealed=False,
        )

    def submit_staging_row(
        self,
        plate_id: str,
        row_number: int,
        row: SubmitRow,
        expected_version: Optional[int],
    ) -> StagingRowReceipt:
        """Submit (or correct, or idempotently retransmit) one staged row.

        * identical retransmit of an existing row returns the stored receipt
          unchanged, regardless of ``expected_version``;
        * a different body requires ``expected_version`` equal to the currently
          stored version, otherwise :class:`RowVersionConflict`;
        * a first submission must not name a version (any version naming is
          rejected because there is nothing the caller could have seen).
        """

        content_hash = canonical_row_hash(row)
        now = utc_iso(datetime.now(timezone.utc))
        idempotent_receipt: Optional[StagingRowReceipt] = None

        with self._write_lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    staging = self._load_staging_header(conn, plate_id)
                    if staging is None:
                        raise StagingNotFound(plate_id)
                    staging_pk, row_count, sealed, sealed_revision = staging
                    if sealed:
                        raise StagingSealed(plate_id, sealed_revision)  # type: ignore[arg-type]
                    if not 1 <= row_number <= row_count:
                        raise RowOutOfRange(plate_id, row_number, row_count)

                    existing = conn.execute(
                        "SELECT id, version, content_hash FROM staging_rows"
                        " WHERE staging_pk = ? AND row_number = ?",
                        (staging_pk, row_number),
                    ).fetchone()

                    if existing is None:
                        if expected_version is not None:
                            raise RowVersionConflict(
                                plate_id, row_number, 0, expected_version
                            )
                        cur = conn.execute(
                            "INSERT INTO staging_rows (staging_pk, row_number,"
                            " version, blank, blank_well, control, control_well,"
                            " content_hash, updated_at)"
                            " VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?)",
                            (
                                staging_pk,
                                row_number,
                                row.blank,
                                row.blank_well,
                                row.control,
                                row.control_well,
                                content_hash,
                                now,
                            ),
                        )
                        row_pk = cur.lastrowid
                        version = 1
                        changed = True
                    else:
                        row_pk, version, stored_hash = existing
                        if stored_hash == content_hash:
                            # Idempotent retransmit: remember the stored row and
                            # fall through to commit, returning it unchanged.
                            idempotent_receipt = self._load_row_receipt(
                                conn, staging_pk, row_number
                            )
                            changed = False
                        else:
                            if expected_version != version:
                                raise RowVersionConflict(
                                    plate_id, row_number, version, expected_version
                                )
                            version += 1
                            conn.execute(
                                "UPDATE staging_rows SET version = ?, blank = ?,"
                                " blank_well = ?, control = ?, control_well = ?,"
                                " content_hash = ?, updated_at = ? WHERE id = ?",
                                (
                                    version,
                                    row.blank,
                                    row.blank_well,
                                    row.control,
                                    row.control_well,
                                    content_hash,
                                    now,
                                    row_pk,
                                ),
                            )
                            conn.execute(
                                "DELETE FROM staging_samples WHERE row_pk = ?",
                                (row_pk,),
                            )
                            changed = True

                    if changed:
                        conn.executemany(
                            "INSERT INTO staging_samples (staging_pk, row_pk,"
                            " sample_id, well, reading) VALUES (?, ?, ?, ?, ?)",
                            [
                                (staging_pk, row_pk, s.sample_id, s.well, s.reading)
                                for s in row.samples
                            ],
                        )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")

        if idempotent_receipt is not None:
            return idempotent_receipt
        with self._connect() as conn:
            return self._load_row_receipt(conn, staging_pk, row_number)

    def seal_staging(
        self, plate_id: str
    ) -> tuple[SubmitResult, bool]:
        """Promote a complete, valid staging record to one stored revision.

        Returns ``(result, already_sealed)``: a repeated seal call returns the
        original receipt (``already_sealed=True``) and never writes another
        revision.  On failure the staging record is kept untouched for
        correction, and no sample reading changes.
        """

        already_sealed = False
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    header = self._load_staging_header(conn, plate_id)
                    if header is None:
                        raise StagingNotFound(plate_id)
                    staging_pk, row_count, sealed, sealed_revision = header

                    if sealed:
                        sealed_result = self._load_sealed_result(
                            conn, plate_id, sealed_revision  # type: ignore[arg-type]
                        )
                        already_sealed = True
                    else:
                        staged = self._load_staged_rows(conn, staging_pk)
                        have_numbers = {receipt.row_number for receipt in staged}
                        missing = [
                            n for n in range(1, row_count + 1)
                            if n not in have_numbers
                        ]
                        if missing:
                            raise StagingIncomplete(
                                plate_id, missing, len(have_numbers)
                            )

                        # Order the rows exactly as the instrument numbered them.
                        by_number = {
                            receipt.row_number: receipt for receipt in staged
                        }
                        ordered = [
                            self._receipt_to_submit_row(by_number[n])
                            for n in range(1, row_count + 1)
                        ]

                        issues = validate_plate_rows(ordered)
                        if issues:
                            raise PlateValidationError(plate_id, issues)

                        tested_at, target_revision = conn.execute(
                            "SELECT tested_at, target_revision"
                            " FROM staging_plates WHERE id = ?",
                            (staging_pk,),
                        ).fetchone()

                        current = conn.execute(
                            "SELECT COALESCE(MAX(revision), 0) FROM plates"
                            " WHERE plate_id = ?",
                            (plate_id,),
                        ).fetchone()[0]
                        if target_revision <= current:
                            raise RevisionConflict(
                                plate_id, current + 1, current
                            )

                        _plate_pk, submitted_at = self._insert_revision(
                            conn,
                            plate_id,
                            target_revision,
                            tested_at,
                            ordered,
                        )
                        conn.execute(
                            "UPDATE staging_plates SET sealed_at = ?,"
                            " sealed_revision = ? WHERE id = ?",
                            (submitted_at, target_revision, staging_pk),
                        )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")

        if already_sealed:
            return sealed_result, True

        result = SubmitResult(
            plate_id=plate_id,
            revision=target_revision,
            tested_at=tested_at,
            submitted_at=submitted_at,
            rows=self._rows_to_out(ordered),
        )
        return result, False

    def get_staging(self, plate_id: str) -> Optional[StagingState]:
        """Return the latest staging record (open or sealed) for a plate."""

        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                header = conn.execute(
                    "SELECT id, target_revision, row_count, tested_at, created_at,"
                    " sealed_at, sealed_revision FROM staging_plates"
                    " WHERE plate_id = ? ORDER BY id DESC LIMIT 1",
                    (plate_id,),
                ).fetchone()
                if header is None:
                    return None
                (
                    staging_pk,
                    target_revision,
                    row_count,
                    tested_at,
                    created_at,
                    sealed_at,
                    sealed_revision,
                ) = header
                rows = conn.execute(
                    "SELECT row_number FROM staging_rows"
                    " WHERE staging_pk = ? ORDER BY row_number",
                    (staging_pk,),
                ).fetchall()
                receipts = tuple(
                    self._load_row_receipt(conn, staging_pk, number)
                    for (number,) in rows
                )
            finally:
                conn.rollback()

        return StagingState(
            plate_id=plate_id,
            target_revision=target_revision,
            row_count=row_count,
            tested_at=tested_at,
            created_at=created_at,
            sealed=sealed_at is not None,
            sealed_at=sealed_at,
            sealed_revision=sealed_revision,
            rows=receipts,
        )

    # ------------------------------------------------------- staging helpers

    @staticmethod
    def _load_staging_header(
        conn: sqlite3.Connection, plate_id: str
    ) -> Optional[tuple[int, int, bool, Optional[int]]]:
        row = conn.execute(
            "SELECT id, row_count, sealed_at, sealed_revision FROM staging_plates"
            " WHERE plate_id = ? ORDER BY id DESC LIMIT 1",
            (plate_id,),
        ).fetchone()
        if row is None:
            return None
        staging_pk, row_count, sealed_at, sealed_revision = row
        return staging_pk, row_count, sealed_at is not None, sealed_revision

    def _load_row_receipt(
        self, conn: sqlite3.Connection, staging_pk: int, row_number: int
    ) -> StagingRowReceipt:
        row = conn.execute(
            "SELECT row_number, version, blank, blank_well, control,"
            " control_well, content_hash, updated_at FROM staging_rows"
            " WHERE staging_pk = ? AND row_number = ?",
            (staging_pk, row_number),
        ).fetchone()
        number, version, blank, blank_well, control, control_well, content_hash, updated_at = row
        samples = conn.execute(
            "SELECT sample_id, well, reading FROM staging_samples"
            " WHERE row_pk = (SELECT id FROM staging_rows"
            " WHERE staging_pk = ? AND row_number = ?) ORDER BY id",
            (staging_pk, row_number),
        ).fetchall()
        submit_row = SubmitRow(
            blank=blank,
            blank_well=blank_well,
            control=control,
            control_well=control_well,
            samples=tuple(
                SampleRow(sample_id=s[0], well=s[1], reading=s[2]) for s in samples
            ),
        )
        return StagingRowReceipt(
            row_number=number,
            version=version,
            content_hash=content_hash,
            row=PlateRowOut(
                row_index=number - 1,
                blank=blank,
                blank_well=blank_well,
                control=control,
                control_well=control_well,
                qualified=submit_row.qualified,
                samples=submit_row.samples,
            ),
            updated_at=updated_at,
        )

    def _load_staged_rows(
        self, conn: sqlite3.Connection, staging_pk: int
    ) -> tuple[StagingRowReceipt, ...]:
        numbers = conn.execute(
            "SELECT row_number FROM staging_rows WHERE staging_pk = ? ORDER BY row_number",
            (staging_pk,),
        ).fetchall()
        return tuple(
            self._load_row_receipt(conn, staging_pk, number) for (number,) in numbers
        )

    @staticmethod
    def _receipt_to_submit_row(receipt: StagingRowReceipt) -> SubmitRow:
        out = receipt.row
        return SubmitRow(
            blank=out.blank,
            blank_well=out.blank_well,
            control=out.control,
            control_well=out.control_well,
            samples=tuple(
                SampleRow(
                    sample_id=s.sample_id, well=s.well, reading=s.reading
                )
                for s in out.samples
            ),
        )

    def _load_sealed_result(
        self, conn: sqlite3.Connection, plate_id: str, revision: int
    ) -> SubmitResult:
        """Rebuild the original seal receipt from the committed revision."""

        plate = conn.execute(
            "SELECT tested_at, submitted_at FROM plates"
            " WHERE plate_id = ? AND revision = ?",
            (plate_id, revision),
        ).fetchone()
        if plate is None:
            # The revision is immutable and never deleted, so this only happens
            # if the database was tampered with; fail loudly rather than invent
            # a receipt.
            raise StagingError(
                "sealed_revision_missing",
                f"sealed revision {revision} of plate {plate_id!r} is missing",
            )
        tested_at, submitted_at = plate
        plate_pk = conn.execute(
            "SELECT id FROM plates WHERE plate_id = ? AND revision = ?",
            (plate_id, revision),
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT row_index, blank, blank_well, control, control_well, qualified"
            " FROM rows WHERE plate_pk = ? ORDER BY row_index",
            (plate_pk,),
        ).fetchall()
        out_rows: list[PlateRowOut] = []
        for index, blank, blank_well, control, control_well, qualified in rows:
            samples = conn.execute(
                "SELECT sample_id, well, reading FROM samples"
                " WHERE row_pk = (SELECT id FROM rows"
                " WHERE plate_pk = ? AND row_index = ?) ORDER BY id",
                (plate_pk, index),
            ).fetchall()
            out_rows.append(
                PlateRowOut(
                    row_index=index,
                    blank=blank,
                    blank_well=blank_well,
                    control=control,
                    control_well=control_well,
                    qualified=bool(qualified),
                    samples=tuple(
                        SampleRow(sample_id=s[0], well=s[1], reading=s[2])
                        for s in samples
                    ),
                )
            )
        return SubmitResult(
            plate_id=plate_id,
            revision=revision,
            tested_at=tested_at,
            submitted_at=submitted_at,
            rows=tuple(out_rows),
        )

    # ------------------------------------------------------------------- reads

    def lookup_sample(self, sample_id: str) -> LookupResult:
        """Resolve the newest usable reading for a sample (see module docstring)."""

        # One read transaction => the two statements see the same snapshot.
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                hits = conn.execute(
                    "SELECT p.plate_id, p.revision, p.tested_at, s.reading, s.well"
                    " FROM samples s"
                    " JOIN plates p ON p.id = s.plate_pk"
                    " JOIN rows r ON r.id = s.row_pk"
                    " WHERE s.sample_id = ? AND r.qualified = 1"
                    "   AND p.revision = ("
                    "       SELECT MAX(p2.revision) FROM plates p2"
                    "       WHERE p2.plate_id = p.plate_id)",
                    (sample_id,),
                ).fetchall()
            finally:
                conn.rollback()  # end the read snapshot

        candidates = [
            QualifiedReading(
                sample_id=sample_id,
                reading=row[3],
                well=row[4],
                plate_id=row[0],
                revision=row[1],
                tested_at=row[2],
            )
            for row in hits
        ]
        if not candidates:
            return LookupResult(sample_id=sample_id, reading=None, conflict=None)

        newest = max(c.tested_at for c in candidates)
        newest_candidates = sorted(
            (c for c in candidates if c.tested_at == newest),
            key=lambda c: (c.plate_id, c.revision),
        )

        if len({c.plate_id for c in newest_candidates}) > 1:
            return LookupResult(
                sample_id=sample_id,
                reading=None,
                conflict=Conflict(
                    sample_id=sample_id,
                    tested_at=newest,
                    candidates=tuple(newest_candidates),
                ),
            )

        # One plate: its latest revision has at most one qualified row for the
        # sample, so this is the single unambiguous answer.
        return LookupResult(
            sample_id=sample_id,
            reading=newest_candidates[0],
            conflict=None,
        )

    def sample_exists(self, sample_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM samples WHERE sample_id = ? LIMIT 1", (sample_id,)
            ).fetchone()
        return row is not None

    def get_plate(self, plate_id: str) -> list[PlateRevision]:
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                plate_rows = conn.execute(
                    "SELECT id, revision, tested_at, submitted_at FROM plates"
                    " WHERE plate_id = ? ORDER BY revision",
                    (plate_id,),
                ).fetchall()
                result: list[PlateRevision] = []
                for pk, revision, tested, submitted in plate_rows:
                    rows = conn.execute(
                        "SELECT id, row_index, blank, blank_well, control,"
                        " control_well, qualified FROM rows"
                        " WHERE plate_pk = ? ORDER BY row_index",
                        (pk,),
                    ).fetchall()
                    out_rows: list[PlateRowOut] = []
                    for (
                        row_pk,
                        index,
                        blank,
                        blank_well,
                        control,
                        control_well,
                        qualified,
                    ) in rows:
                        samples = conn.execute(
                            "SELECT sample_id, well, reading FROM samples"
                            " WHERE row_pk = ? ORDER BY id",
                            (row_pk,),
                        ).fetchall()
                        out_rows.append(
                            PlateRowOut(
                                row_index=index,
                                blank=blank,
                                blank_well=blank_well,
                                control=control,
                                control_well=control_well,
                                qualified=bool(qualified),
                                samples=tuple(
                                    SampleRow(
                                        sample_id=s[0], well=s[1], reading=s[2]
                                    )
                                    for s in samples
                                ),
                            )
                        )
                    result.append(
                        PlateRevision(
                            revision=revision,
                            tested_at=tested,
                            submitted_at=submitted,
                            rows=tuple(out_rows),
                        )
                    )
            finally:
                conn.rollback()
        return result
