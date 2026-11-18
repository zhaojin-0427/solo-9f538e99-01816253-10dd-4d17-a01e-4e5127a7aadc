"""End-to-end tests for the row-by-row staging API."""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def row(blank, control, samples, prefix="A"):
    return {
        "blank": {"well": f"{prefix}1", "reading": blank},
        "positive_control": {"well": f"{prefix}2", "reading": control},
        "samples": [
            {"well": f"{prefix}{i + 3}", "sample_id": sid, "reading": reading}
            for i, (sid, reading) in enumerate(samples)
        ],
    }


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def register(client, plate_id, revision, tested_at, row_count):
    return client.post(
        f"/plates/{plate_id}/staging",
        json={
            "target_revision": revision,
            "tested_at": tested_at,
            "row_count": row_count,
        },
    )


def send_row(client, plate_id, row_number, row_body, expected_version=None):
    payload: dict = {"row": row_body}
    if expected_version is not None:
        payload["expected_version"] = expected_version
    return client.put(
        f"/plates/{plate_id}/staging/rows/{row_number}", json=payload
    )


def seal(client, plate_id):
    return client.post(f"/plates/{plate_id}/staging/seal")


def status(client, plate_id):
    return client.get(f"/plates/{plate_id}/staging")


# ----------------------------------------------------------- registration


def test_register_and_inspect_empty_staging(client):
    r = register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["plate_id"] == "P1"
    assert body["target_revision"] == 1
    assert body["row_count"] == 2
    assert body["sealed"] is False
    assert body["received_rows"] == []
    assert body["missing_rows"] == [1, 2]

    got = status(client, "P1")
    assert got.status_code == 200
    assert got.json()["missing_rows"] == [1, 2]


def test_register_requires_row_count_1_to_4(client):
    for bad in (0, 5):
        r = client.post(
            "/plates/P1/staging",
            json={
                "target_revision": 1,
                "tested_at": "2024-01-01T00:00:00Z",
                "row_count": bad,
            },
        )
        assert r.status_code == 422, bad


def test_second_open_registration_is_conflict(client):
    assert register(client, "P1", 1, "2024-01-01T00:00:00Z", 1).status_code == 201
    r = register(client, "P1", 2, "2024-01-02T00:00:00Z", 1)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "staging_active"


def test_registration_allowed_again_after_sealing(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    assert seal(client, "P1").status_code == 201

    r = register(client, "P1", 2, "2024-02-01T00:00:00Z", 1)
    assert r.status_code == 201, r.text
    assert r.json()["sealed"] is False


def test_unknown_staging_is_404(client):
    r = status(client, "nope")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "staging_not_found"
    assert send_row(
        client, "nope", 1, row(0.02, 1.0, [("S1", 0.3)])
    ).status_code == 404
    assert seal(client, "nope").status_code == 404


# ------------------------------------------------------------- row writes


def test_first_row_needs_no_version_and_starts_at_version_1(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    r = send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["row_number"] == 1
    assert body["version"] == 1
    assert body["row"]["qualified"] is True

    got = status(client, "P1").json()
    assert got["received_rows"] == [1]
    assert got["missing_rows"] == [2]
    assert got["rows"][0]["version"] == 1


def test_rows_may_arrive_out_of_order(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 3)
    assert send_row(
        client, "P1", 3, row(0.02, 1.0, [("S3", 0.3)], prefix="C")
    ).status_code == 200
    assert send_row(
        client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A")
    ).status_code == 200
    assert status(client, "P1").json()["missing_rows"] == [2]
    assert send_row(
        client, "P1", 2, row(0.02, 1.0, [("S2", 0.3)], prefix="B")
    ).status_code == 200
    assert status(client, "P1").json()["missing_rows"] == []


def test_row_number_out_of_declared_range_rejected(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    r = send_row(
        client, "P1", 3, row(0.02, 1.0, [("S1", 0.3)], prefix="C")
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "row_out_of_range"


def test_identical_retransmit_returns_original_row(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    first = send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.30)]))
    assert first.status_code == 200
    original = first.json()

    # Same content, different whitespace-equivalent spelling and even with a
    # stale version hint: idempotent, the stored row is returned untouched.
    retry = send_row(
        client,
        "P1",
        1,
        row(0.02, 1.0, [("S1", 0.3)]),
        expected_version=99,
    )
    assert retry.status_code == 200
    again = retry.json()
    assert again["version"] == original["version"] == 1
    assert again["content_hash"] == original["content_hash"]
    assert again["updated_at"] == original["updated_at"]


def test_correction_requires_seen_version(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.9, 1.0, [("S1", 0.3)]))

    # Different content, no version -> rejected.
    r = send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "row_version_conflict"
    assert detail["current_version"] == 1
    assert detail["expected_version"] is None

    # A wrong (stale) version is rejected as well.
    r = send_row(
        client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]), expected_version=2
    )
    assert r.status_code == 409
    assert r.json()["detail"]["current_version"] == 1

    # The original row is still there.
    assert status(client, "P1").json()["rows"][0]["row"]["blank"]["reading"] == 0.9

    # Correct version -> accepted, version bumps to 2.
    r = send_row(
        client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]), expected_version=1
    )
    assert r.status_code == 200
    assert r.json()["version"] == 2
    assert status(client, "P1").json()["rows"][0]["version"] == 2


def test_naming_a_version_on_first_submission_is_rejected(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    r = send_row(
        client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]), expected_version=1
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "row_version_conflict"


def test_row_validation_rules_apply_to_staged_rows(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    bad = row(0.02, 1.0, [("S1", -1)])
    assert send_row(client, "P1", 1, bad).status_code == 422


def test_inline_row_shape_is_accepted(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    payload = {
        "blank": {"well": "A1", "reading": 0.02},
        "positive_control": {"well": "A2", "reading": 1.0},
        "samples": [{"well": "A3", "sample_id": "S1", "reading": 0.3}],
    }
    r = client.put("/plates/P1/staging/rows/1", json=payload)
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 1


# ---------------------------------------------------------------- sealing


def test_seal_stores_complete_plate_as_target_revision(client):
    register(client, "P1", 1, "2024-01-01T09:30:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    send_row(client, "P1", 2, row(0.03, 1.05, [("S2", 0.4)], prefix="B"))

    r = seal(client, "P1")
    assert r.status_code == 201, r.text
    receipt = r.json()
    assert receipt["plate_id"] == "P1"
    assert receipt["revision"] == 1
    assert receipt["qualified_rows"] == 2
    assert len(receipt["rows"]) == 2

    got = client.get("/samples/S1/reading")
    assert got.status_code == 200
    assert got.json()["plate_id"] == "P1"
    assert got.json()["revision"] == 1

    state = status(client, "P1").json()
    assert state["sealed"] is True
    assert state["sealed_revision"] == 1
    assert state["sealed_at"] is not None


def test_seal_with_missing_rows_changes_nothing(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))

    r = seal(client, "P1")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "staging_incomplete"
    assert detail["missing_rows"] == [2]

    # Nothing entered the revision history, no sample is served, staging kept.
    assert client.get("/plates/P1").status_code == 404
    assert client.get("/samples/S1/reading").status_code == 404
    state = status(client, "P1").json()
    assert state["sealed"] is False
    assert state["received_rows"] == [1]

    # Complete it and seal successfully.
    send_row(client, "P1", 2, row(0.02, 1.0, [("S2", 0.4)], prefix="B"))
    assert seal(client, "P1").status_code == 201


def test_seal_retry_returns_original_receipt_without_new_revision(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    first = seal(client, "P1")
    assert first.status_code == 201
    original = first.json()

    retry = seal(client, "P1")
    assert retry.status_code == 200
    assert retry.json() == original

    history = client.get("/plates/P1").json()
    assert [r["revision"] for r in history["revisions"]] == [1]


def test_sealed_staging_rejects_further_row_writes(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    seal(client, "P1")

    r = send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.4)]))
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "staging_sealed"


def test_seal_runs_plate_wide_uniqueness_validation_and_keeps_staging(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    # Same sample id on the second row -> plate-wide duplicate.
    send_row(client, "P1", 2, row(0.02, 1.0, [("S1", 0.4)], prefix="B"))

    r = seal(client, "P1")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "plate_validation_failed"
    assert any("S1" in issue for issue in detail["issues"])

    # Staging retained for correction; no revision stored.
    assert client.get("/plates/P1").status_code == 404
    assert status(client, "P1").json()["sealed"] is False

    # Correct row 2 using the seen version and seal again.
    corrected = send_row(
        client,
        "P1",
        2,
        row(0.02, 1.0, [("S2", 0.4)], prefix="B"),
        expected_version=1,
    )
    assert corrected.status_code == 200
    assert seal(client, "P1").status_code == 201


def test_stale_target_revision_blocks_seal_but_keeps_staging(client):
    # A whole-plate submission takes revision 1 first.
    whole = client.post(
        "/plates/P1/revisions",
        json={
            "plate_id": "P1",
            "revision": 1,
            "tested_at": "2024-01-01T00:00:00Z",
            "rows": [row(0.02, 1.0, [("S9", 0.9)])],
        },
    )
    assert whole.status_code == 201

    register(client, "P1", 1, "2024-02-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))

    r = seal(client, "P1")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "revision_conflict"
    assert r.json()["detail"]["latest_revision"] == 1

    # Current sample reading is untouched and staging remains correctable.
    assert client.get("/samples/S9/reading").status_code == 200
    assert client.get("/samples/S1/reading").status_code == 404
    state = status(client, "P1").json()
    assert state["sealed"] is False
    assert state["rows"][0]["row"]["samples"][0]["sample_id"] == "S1"


def test_incomplete_staging_is_invisible_to_analysis(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    # Half plate must never show up under the sample lookup.
    assert client.get("/samples/S1/reading").status_code == 404
    assert client.get("/plates/P1").status_code == 404


def test_quality_gate_applies_to_sealed_plate(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.5, 1.0, [("S1", 0.3)]))
    assert seal(client, "P1").status_code == 201

    got = client.get("/samples/S1/reading")
    assert got.status_code == 404
    assert got.json()["detail"]["error"] == "no_qualified_reading"


# ----------------------------------------------------------- concurrency


def test_concurrent_different_rows_all_land_once(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 4)
    errors = []

    def worker(number):
        prefix = "ABCD"[number - 1]
        for _ in range(5):  # include retries, all identical
            r = send_row(
                client,
                "P1",
                number,
                row(0.02, 1.0, [(f"S{number}", 0.3)], prefix=prefix),
            )
            if r.status_code != 200:
                errors.append((number, r.status_code, r.text))

    threads = [
        threading.Thread(target=worker, args=(n,)) for n in (1, 2, 3, 4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    state = status(client, "P1").json()
    assert state["missing_rows"] == []
    assert {r["row_number"]: r["version"] for r in state["rows"]} == {
        1: 1,
        2: 1,
        3: 1,
        4: 1,
    }
    assert seal(client, "P1").status_code == 201


def test_concurrent_corrections_only_one_wins_version_race(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.9, 1.0, [("S1", 0.3)]))
    codes = []
    lock = threading.Lock()

    def worker(blank):
        r = send_row(
            client,
            "P1",
            1,
            row(blank, 1.0, [("S1", 0.3)]),
            expected_version=1,
        )
        with lock:
            codes.append(r.status_code)

    threads = [threading.Thread(target=worker, args=(b,)) for b in (0.01, 0.02)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(codes) == [200, 409]
    assert status(client, "P1").json()["rows"][0]["version"] == 2


def test_concurrent_seal_and_whole_plate_submission(client):
    register(client, "P1", 2, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    outcomes = []
    lock = threading.Lock()

    def seal_worker():
        r = seal(client, "P1")
        with lock:
            outcomes.append(("seal", r.status_code))

    def whole_worker():
        r = client.post(
            "/plates/P1/revisions",
            json={
                "plate_id": "P1",
                "revision": 2,
                "tested_at": "2024-02-01T00:00:00Z",
                "rows": [row(0.02, 1.0, [("S2", 0.5)])],
            },
        )
        with lock:
            outcomes.append(("whole", r.status_code))

    threads = [threading.Thread(target=seal_worker), threading.Thread(target=whole_worker)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = dict(outcomes)
    # Exactly one writer wins revision 2; the loser is a 409 and nothing is half stored.
    assert sorted(statuses.values()) == [201, 409]
    history = client.get("/plates/P1").json()
    assert [r["revision"] for r in history["revisions"]] == [2]

    # Whichever lost, retrying the seal must not create another revision.
    if statuses["seal"] == 409:
        # Staging is retained and still unsealed; its target revision is now stale.
        assert status(client, "P1").json()["sealed"] is False
        retry = seal(client, "P1")
        assert retry.status_code == 409
    else:
        assert seal(client, "P1").status_code == 200
    assert [r["revision"] for r in client.get("/plates/P1").json()["revisions"]] == [2]


def test_concurrent_seals_create_one_revision(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    codes = []
    lock = threading.Lock()

    def worker():
        r = seal(client, "P1")
        with lock:
            codes.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(codes) == [200] * 5 + [201]
    assert [r["revision"] for r in client.get("/plates/P1").json()["revisions"]] == [1]


# ------------------------------------------------------------ persistence


def test_staging_survives_restart(tmp_path):
    path = str(tmp_path / "persist.db")
    with TestClient(create_app(path)) as c:
        register(c, "P1", 1, "2024-01-01T00:00:00Z", 2)
        send_row(c, "P1", 2, row(0.02, 1.0, [("S2", 0.4)], prefix="B"))

    with TestClient(create_app(path)) as c2:
        state = status(c2, "P1").json()
        assert state["sealed"] is False
        assert state["received_rows"] == [2]
        assert state["missing_rows"] == [1]
        assert state["rows"][0]["version"] == 1

        # Finish and seal after the restart.
        send_row(c2, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
        r = seal(c2, "P1")
        assert r.status_code == 201
        assert r.json()["revision"] == 1

    with TestClient(create_app(path)) as c3:
        state = status(c3, "P1").json()
        assert state["sealed"] is True
        assert state["sealed_revision"] == 1
        # Sealed rows remain queryable with their final versions.
        assert {r["row_number"]: r["version"] for r in state["rows"]} == {
            1: 1,
            2: 1,
        }
        # A repeated seal after restart replays the original receipt.
        retry = seal(c3, "P1")
        assert retry.status_code == 200
        assert retry.json()["revision"] == 1
        assert [
            r["revision"]
            for r in c3.get("/plates/P1").json()["revisions"]
        ] == [1]


def test_docs_list_staging_endpoints(client):
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    assert "/plates/{plate_id}/staging" in paths
    assert "/plates/{plate_id}/staging/rows/{row_number}" in paths
    assert "/plates/{plate_id}/staging/seal" in paths
    assert client.get("/docs").status_code == 200


def test_failed_seal_can_be_fixed_and_resealed(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    send_row(client, "P1", 2, row(0.02, 1.0, [("S1", 0.4)], prefix="B"))
    assert seal(client, "P1").status_code == 409  # duplicate sample id

    # Correct both rows through the version protocol, then the seal succeeds.
    fix1 = send_row(
        client, "P1", 1, row(0.02, 1.0, [("S1", 0.31)], prefix="A"),
        expected_version=1,
    )
    assert fix1.status_code == 200 and fix1.json()["version"] == 2
    fix2 = send_row(
        client, "P1", 2, row(0.02, 1.0, [("S2", 0.41)], prefix="B"),
        expected_version=1,
    )
    assert fix2.status_code == 200 and fix2.json()["version"] == 2

    assert seal(client, "P1").status_code == 201
    got = client.get("/samples/S2/reading")
    assert got.status_code == 200 and got.json()["reading"] == 0.41


def test_registration_aliases_and_naive_timestamp(client):
    r = client.post(
        "/plates/P1/staging",
        json={
            "revision": 2,
            "testedAt": "2024-01-01T02:00:00",
            "rowCount": 1,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["target_revision"] == 2
    assert body["tested_at"] == "2024-01-01T02:00:00Z"


def test_expected_version_must_be_positive(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    r = client.put(
        "/plates/P1/staging/rows/1",
        json={
            "expected_version": 0,
            "row": row(0.02, 1.0, [("S1", 0.3)]),
        },
    )
    assert r.status_code == 422


def test_row_path_number_above_four_is_422(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 1)
    r = client.put(
        "/plates/P1/staging/rows/5",
        json={"row": row(0.02, 1.0, [("S1", 0.3)])},
    )
    assert r.status_code == 422


def test_sealed_history_lists_revision_and_lookup_serves_it(client):
    register(client, "P1", 7, "2024-01-01T00:00:00Z", 1)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)]))
    seal(client, "P1")

    history = client.get("/plates/P1").json()
    assert history["latest_revision"] == 7
    assert [r["revision"] for r in history["revisions"]] == [7]
    assert client.get("/samples/S1/reading").json()["revision"] == 7


def test_well_uniqueness_failure_also_blocks_seal(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    # Reuse well A1 from row 1 on row 2.
    duplicate = {
        "blank": {"well": "A1", "reading": 0.02},
        "positive_control": {"well": "B2", "reading": 1.0},
        "samples": [{"well": "B3", "sample_id": "S2", "reading": 0.4}],
    }
    assert send_row(client, "P1", 2, duplicate).status_code == 200
    r = seal(client, "P1")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "plate_validation_failed"
    assert client.get("/plates/P1").status_code == 404


def test_qualified_flags_per_row_are_persisted_in_staging(client):
    register(client, "P1", 1, "2024-01-01T00:00:00Z", 2)
    send_row(client, "P1", 1, row(0.02, 1.0, [("S1", 0.3)], prefix="A"))
    send_row(client, "P1", 2, row(0.5, 1.0, [("S2", 0.4)], prefix="B"))
    rows = {r["row_number"]: r["row"]["qualified"]
            for r in status(client, "P1").json()["rows"]}
    assert rows == {1: True, 2: False}

    seal(client, "P1")
    receipt = client.get("/plates/P1").json()["revisions"][0]
    assert [r["qualified"] for r in receipt["rows"]] == [True, False]
    assert client.get("/samples/S1/reading").status_code == 200
    assert client.get("/samples/S2/reading").status_code == 404
