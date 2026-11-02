from __future__ import annotations

import os
import shutil

from app.maintenance import latest_snapshot, snapshot, verify
from app.sync import process


def test_snapshot_verify_and_restore(harness, tmp_path):
    harness.syncer().run("backfill", backfill=True)
    process(harness.settings, harness.ledger)
    data = harness.settings.data_dir
    snap = snapshot(data)
    assert snap == latest_snapshot(data)
    report = verify(data, smoke=True)
    assert report["ok"], report["problems"]
    assert report["files_checked"] > 100 and report["smoke"]["stream_records"] == 60

    # Restore: copy the backup (without the live ledger) and put the snapshot in place.
    restored = tmp_path / "restored"
    shutil.copytree(data, restored, ignore=shutil.ignore_patterns("ledger.sqlite"))
    shutil.copy(latest_snapshot(restored), restored / "db" / "ledger.sqlite")
    report = verify(restored, smoke=True)
    assert report["ok"], report["problems"]
    assert report["smoke"]["backfill_complete"] is True

    # Corruption and loss are detected.
    victim = next((restored / "raw" / "fit").rglob("*.fit"))
    os.chmod(victim, 0o644)
    victim.write_bytes(b"corrupted")
    next(p for p in (restored / "raw" / "api").rglob("*.json")
         if not p.name.endswith(".meta.json")).unlink()
    report = verify(restored)
    assert not report["ok"]
    assert report["hash_mismatches"] == 1 and report["files_missing"] >= 1
