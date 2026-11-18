"""FastAPI application: microplate submission and qualified sample lookup."""

import os
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Path, Request, Response, status

from .models import (
    ConflictCandidateOut,
    ConflictDetail,
    PlateNotFoundDetail,
    PlateOut,
    RevisionConflictDetail,
    PlateRevisionOut,
    PlateSubmission,
    RowOut,
    SampleNotFoundDetail,
    SampleOut,
    SampleReadingOut,
    StagingErrorDetail,
    StagingRegistrationIn,
    StagingRowIn,
    StagingRowReceiptOut,
    StagingStatusOut,
    SubmissionOut,
)
from .storage import (
    PlateRevision,
    PlateRowOut,
    PlateValidationError,
    RevisionConflict,
    RowOutOfRange,
    RowVersionConflict,
    SampleRow,
    StagingActive,
    StagingError,
    StagingIncomplete,
    StagingNotFound,
    StagingRowReceipt,
    StagingSealed,
    StagingState,
    Store,
    SubmitRow,
    parse_utc,
)

PlateId = Annotated[
    str, Path(min_length=1, max_length=128, description="Plate identifier")
]
SampleId = Annotated[
    str, Path(min_length=1, max_length=128, description="Sample identifier")
]

DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "plates.db"
)

DESCRIPTION = """
Reliable readings for downstream analysis.

* Submit a whole plate (1-4 rows) as a new revision via
  `POST /plates/{plate_id}/revisions`. Revisions start at 1 and increase; a
  submission is atomic, so a failed request changes nothing.
* Instruments that deliver one row at a time can stage the plate:
  register with `POST /plates/{plate_id}/staging`, send rows (in any order,
  with retries) via `PUT /plates/{plate_id}/staging/rows/{row_number}`, then
  seal with `POST /plates/{plate_id}/staging/seal`. The analysis endpoints
  never see a half plate — the whole plate enters the revision history only
  when the seal succeeds.
* A row is **qualified** only when its blank is strictly below `0.1` and its
  positive control is within `[0.9, 1.1]`. Unqualified rows are stored and
  reported but never served to analysis.
* `GET /samples/{sample_id}/reading` returns the qualified reading with the
  newest `tested_at` across plates. A newer failed plate does not displace an
  older qualified reading. If two different qualified plates share the newest
  `tested_at`, the response is `409 ambiguous_reading` and no reading is served.
* The newest revision of a plate is authoritative, so correcting a plate
  immediately re-judges every sample it contains.
"""


def _submit_row(row) -> SubmitRow:
    return SubmitRow(
        blank=row.blank.reading,
        blank_well=row.blank.well,
        control=row.positive_control.reading,
        control_well=row.positive_control.well,
        samples=[
            SampleRow(sample_id=s.sample_id, well=s.well, reading=s.reading)
            for s in row.samples
        ],
    )


def _submit_rows(plate: PlateSubmission) -> list[SubmitRow]:
    return [_submit_row(row) for row in plate.rows]


def _row_out(row: PlateRowOut) -> RowOut:
    return RowOut(
        blank={"well": row.blank_well, "reading": row.blank},
        positive_control={"well": row.control_well, "reading": row.control},
        qualified=row.qualified,
        samples=[
            SampleOut(well=s.well, sample_id=s.sample_id, reading=s.reading)
            for s in row.samples
        ],
    )


def _submission_out(result) -> SubmissionOut:
    return SubmissionOut(
        plate_id=result.plate_id,
        revision=result.revision,
        tested_at=parse_utc(result.tested_at),
        submitted_at=parse_utc(result.submitted_at),
        qualified_rows=result.qualified_rows,
        rejected_rows=result.rejected_rows,
        rows=[_row_out(row) for row in result.rows],
    )


def _staging_receipt_out(receipt: StagingRowReceipt) -> StagingRowReceiptOut:
    return StagingRowReceiptOut(
        row_number=receipt.row_number,
        version=receipt.version,
        content_hash=receipt.content_hash,
        updated_at=parse_utc(receipt.updated_at),
        row=_row_out(receipt.row),
    )


def _staging_status_out(state: StagingState) -> StagingStatusOut:
    return StagingStatusOut(
        plate_id=state.plate_id,
        target_revision=state.target_revision,
        row_count=state.row_count,
        tested_at=parse_utc(state.tested_at),
        created_at=parse_utc(state.created_at),
        sealed=state.sealed,
        sealed_at=parse_utc(state.sealed_at) if state.sealed_at else None,
        sealed_revision=state.sealed_revision,
        received_rows=list(state.received_row_numbers),
        missing_rows=list(state.missing_row_numbers),
        rows=[_staging_receipt_out(r) for r in state.rows],
    )


def _staging_error(plate_id: str, exc: StagingError) -> HTTPException:
    detail_kwargs: dict = {"error": exc.code, "message": str(exc), "plate_id": plate_id}
    if isinstance(exc, RowVersionConflict):
        detail_kwargs["current_version"] = exc.current_version
        detail_kwargs["expected_version"] = exc.expected_version
    if isinstance(exc, StagingIncomplete):
        detail_kwargs["missing_rows"] = list(exc.missing)
    if isinstance(exc, PlateValidationError):
        detail_kwargs["issues"] = list(exc.issues)
    if isinstance(exc, StagingSealed):
        detail_kwargs["sealed_revision"] = exc.sealed_revision
    detail = StagingErrorDetail(**detail_kwargs)
    http_status = (
        status.HTTP_404_NOT_FOUND
        if isinstance(exc, StagingNotFound)
        else status.HTTP_409_CONFLICT
    )
    return HTTPException(status_code=http_status, detail=detail.model_dump(mode="json"))


def _plate_revision_out(revision: PlateRevision) -> PlateRevisionOut:
    return PlateRevisionOut(
        revision=revision.revision,
        tested_at=parse_utc(revision.tested_at),
        submitted_at=parse_utc(revision.submitted_at),
        rows=[_row_out(row) for row in revision.rows],
    )


def create_app(db_path: str | None = None) -> FastAPI:
    store = Store(db_path or os.environ.get("DATABASE_PATH", DEFAULT_DB_PATH))

    app = FastAPI(
        title="Microplate Readings API",
        version="1.0.0",
        description=DESCRIPTION,
    )
    app.state.store = store

    def get_store(request: Request) -> Store:
        return request.app.state.store

    StoreDep = Annotated[Store, Depends(get_store)]

    @app.get("/health", tags=["ops"], summary="Liveness probe")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/plates/{plate_id}/revisions",
        response_model=SubmissionOut,
        status_code=status.HTTP_201_CREATED,
        tags=["plates"],
        summary="Submit a plate revision",
        responses={
            409: {
                "description": "Revision is not the next revision for this plate; "
                "nothing was stored.",
                "model": None,
            }
        },
    )
    def submit_plate(
        plate_id: PlateId,
        submission: PlateSubmission,
        store: StoreDep,
    ) -> SubmissionOut:
        if submission.plate_id != plate_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="plate_id in body does not match the path",
            )

        try:
            result = store.submit_plate(
                plate_id,
                submission.revision,
                submission.tested_at,
                _submit_rows(submission),
            )
        except RevisionConflict as exc:
            detail = RevisionConflictDetail(
                message=(
                    f"plate {plate_id!r} is at revision {exc.latest}; "
                    f"submit a revision greater than {exc.latest} "
                    f"(next available: {exc.next_revision})"
                ),
                plate_id=plate_id,
                submitted_revision=submission.revision,
                latest_revision=exc.latest,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=detail.model_dump(mode="json"),
            ) from exc

        return _submission_out(result)

    @app.get(
        "/plates/{plate_id}",
        response_model=PlateOut,
        tags=["plates"],
        summary="List the stored revisions of a plate",
        responses={404: {"description": "Unknown plate", "model": None}},
    )
    def get_plate(plate_id: PlateId, store: StoreDep) -> PlateOut:
        revisions = store.get_plate(plate_id)
        if not revisions:
            detail = PlateNotFoundDetail(
                message=f"plate {plate_id!r} has no revisions",
                plate_id=plate_id,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=detail.model_dump(mode="json"),
            )
        return PlateOut(
            plate_id=plate_id,
            latest_revision=revisions[-1].revision,
            revisions=[_plate_revision_out(r) for r in revisions],
        )

    @app.get(
        "/samples/{sample_id}/reading",
        response_model=SampleReadingOut,
        tags=["samples"],
        summary="Get the newest qualified reading for a sample",
        responses={
            404: {"description": "Sample unknown or no qualified reading", "model": None},
            409: {
                "description": "Two different qualified plates share the newest "
                "tested_at; no reading is served.",
                "model": None,
            },
        },
    )
    def get_sample_reading(sample_id: SampleId, store: StoreDep) -> SampleReadingOut:
        result = store.lookup_sample(sample_id)

        if result.conflict is not None:
            conflict = result.conflict
            detail = ConflictDetail(
                message=(
                    f"sample {sample_id!r} has qualified readings from multiple "
                    f"plates tested at {conflict.tested_at}; refusing to choose"
                ),
                sample_id=sample_id,
                tested_at=parse_utc(conflict.tested_at),
                candidates=[
                    ConflictCandidateOut(
                        plate_id=c.plate_id,
                        revision=c.revision,
                        well=c.well,
                        reading=c.reading,
                    )
                    for c in conflict.candidates
                ],
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=detail.model_dump(mode="json"),
            )

        if result.reading is None:
            if store.sample_exists(sample_id):
                message = (
                    f"sample {sample_id!r} exists but has no qualified reading"
                )
                error = "no_qualified_reading"
            else:
                message = f"sample {sample_id!r} was never submitted"
                error = "sample_not_found"
            detail = SampleNotFoundDetail(
                error=error, message=message, sample_id=sample_id
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=detail.model_dump(mode="json"),
            )

        reading = result.reading
        return SampleReadingOut(
            sample_id=reading.sample_id,
            reading=reading.reading,
            well=reading.well,
            plate_id=reading.plate_id,
            revision=reading.revision,
            tested_at=parse_utc(reading.tested_at),
        )

    # ------------------------------------------------------------- staging

    @app.post(
        "/plates/{plate_id}/staging",
        response_model=StagingStatusOut,
        status_code=status.HTTP_201_CREATED,
        tags=["staging"],
        summary="Register a plate for row-by-row staging",
        responses={
            409: {
                "description": "An open staging record already exists for this plate.",
                "model": None,
            }
        },
    )
    def register_staging(
        plate_id: PlateId, body: StagingRegistrationIn, store: StoreDep
    ) -> StagingStatusOut:
        try:
            state = store.register_staging(
                plate_id,
                body.target_revision,
                body.tested_at,
                body.row_count,
            )
        except StagingActive as exc:
            raise _staging_error(plate_id, exc) from exc
        return _staging_status_out(state)

    @app.put(
        "/plates/{plate_id}/staging/rows/{row_number}",
        response_model=StagingRowReceiptOut,
        tags=["staging"],
        summary="Submit, correct or idempotently retransmit one staged row",
        responses={
            404: {"description": "No staging record for this plate", "model": None},
            409: {
                "description": "Sealed plate, row number out of range, or a "
                "correction without the matching expected_version.",
                "model": None,
            },
        },
    )
    def submit_staging_row(
        plate_id: PlateId,
        row_number: Annotated[int, Path(ge=1, le=4, description="1-based row number")],
        body: StagingRowIn,
        store: StoreDep,
    ) -> StagingRowReceiptOut:
        try:
            receipt = store.submit_staging_row(
                plate_id,
                row_number,
                _submit_row(body.row),
                body.expected_version,
            )
        except (StagingNotFound, StagingSealed, RowOutOfRange, RowVersionConflict) as exc:
            raise _staging_error(plate_id, exc) from exc
        return _staging_receipt_out(receipt)

    @app.post(
        "/plates/{plate_id}/staging/seal",
        response_model=SubmissionOut,
        tags=["staging"],
        summary="Seal staging and store the complete plate as one revision",
        responses={
            200: {
                "description": "Staging was already sealed; the original receipt "
                "is returned again without creating another revision.",
                "model": SubmissionOut,
            },
            404: {"description": "No staging record for this plate", "model": None},
            409: {
                "description": "Rows missing, target revision no longer ahead, or "
                "plate-wide validation failed. The staging record is retained.",
                "model": None,
            },
        },
    )
    def seal_staging(
        plate_id: PlateId, store: StoreDep, response: Response
    ) -> SubmissionOut:
        try:
            result, already_sealed = store.seal_staging(plate_id)
        except RevisionConflict as exc:
            detail = RevisionConflictDetail(
                message=(
                    f"target revision no longer ahead: plate {plate_id!r} is at "
                    f"revision {exc.latest}; correct the rows and seal with a "
                    f"revision greater than {exc.latest} "
                    f"(next available: {exc.next_revision})"
                ),
                plate_id=plate_id,
                submitted_revision=None,
                latest_revision=exc.latest,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=detail.model_dump(mode="json"),
            ) from exc
        except (
            StagingNotFound,
            StagingIncomplete,
            PlateValidationError,
            StagingError,
        ) as exc:
            raise _staging_error(plate_id, exc) from exc

        # First seal creates a revision (201); a retry just replays the
        # original receipt (200) and never produces another version.
        response.status_code = (
            status.HTTP_200_OK if already_sealed else status.HTTP_201_CREATED
        )
        return _submission_out(result)

    @app.get(
        "/plates/{plate_id}/staging",
        response_model=StagingStatusOut,
        tags=["staging"],
        summary="Inspect the staging state of a plate",
        responses={404: {"description": "No staging record", "model": None}},
    )
    def get_staging(plate_id: PlateId, store: StoreDep) -> StagingStatusOut:
        state = store.get_staging(plate_id)
        if state is None:
            detail = StagingErrorDetail(
                error="staging_not_found",
                message=f"plate {plate_id!r} has no staging record",
                plate_id=plate_id,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=detail.model_dump(mode="json"),
            )
        return _staging_status_out(state)

    return app


app = create_app()
