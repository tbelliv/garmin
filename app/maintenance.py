"""Backup support and integrity verification.

  python -m app.maintenance snapshot [--keep 14]
      Consistent copy of the SQLite ledger into db/snapshots/ (SQLite backup API),
      safe while the sync is running.  Backups copy the snapshot, not the live file.

  python -m app.maintenance verify [--root DIR] [--ledger FILE] [--smoke]
      Re-hash every archived file against the ledger manifest, run SQLite's
      integrity check, open every Parquet file, and with --smoke run the MCP
      query layer against the data.  Exit status 1 on any problem.  Used both
      on the live archive and on a restored backup.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .archive import sha256_file
from .settings import load_settings


def snapshot(data_dir: Path, keep: int = 14) -> Path:
    src = data_dir / "db" / "ledger.sqlite"
    if not src.exists():
        raise SystemExit(f"no ledger at {src}")
    out_dir = data_dir / "db" / "snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = out_dir / f"ledger-{stamp}.sqlite"
    tmp = target.with_suffix(".tmp")
    with sqlite3.connect(f"file:{src}?mode=ro", uri=True) as source, \
            sqlite3.connect(tmp) as dest:
        source.backup(dest)
    tmp.replace(target)
    snaps = sorted(out_dir.glob("ledger-*.sqlite"))
    for old in snaps[:-keep]:
        old.unlink()
    return target


def latest_snapshot(data_dir: Path) -> Path | None:
    snaps = sorted((data_dir / "db" / "snapshots").glob("ledger-*.sqlite"))
    return snaps[-1] if snaps else None


def verify(root: Path, ledger_path: Path | None = None, smoke: bool = False) -> dict[str, Any]:
    ledger_path = ledger_path or root / "db" / "ledger.sqlite"
    if not ledger_path.exists():
        ledger_path = latest_snapshot(root) or ledger_path
    result: dict[str, Any] = {"root": str(root), "ledger": str(ledger_path), "problems": []}
    problems: list[str] = result["problems"]
    if not ledger_path.exists():
        problems.append("ledger not found")
        return result
    con = sqlite3.connect(f"file:{ledger_path}?mode=ro", uri=True)
    integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
    result["sqlite_integrity"] = integrity
    if integrity != "ok":
        problems.append(f"sqlite integrity: {integrity}")
    checked = missing = mismatched = unreadable = 0
    total_bytes = 0
    for rel, digest, size in con.execute("SELECT path, sha256, bytes FROM archive_files"):
        path = root / rel
        checked += 1
        if not path.exists():
            missing += 1
            if missing <= 20:
                problems.append(f"missing: {rel}")
            continue
        try:
            actual = sha256_file(path)
        except OSError as exc:
            unreadable += 1
            if unreadable <= 20:
                problems.append(f"unreadable: {rel}: {exc.strerror}")
            continue
        if actual != digest:
            mismatched += 1
            if mismatched <= 20:
                problems.append(f"hash mismatch: {rel}")
        total_bytes += size
    result.update(files_checked=checked, files_missing=missing, hash_mismatches=mismatched,
                  files_unreadable=unreadable, bytes_verified=total_bytes)
    listed = {r[0] for r in con.execute("SELECT path FROM archive_files")}
    unlisted = [str(p.relative_to(root)) for p in (root / "raw").rglob("*")
                if p.is_file() and not p.name.endswith(".meta.json")
                and str(p.relative_to(root)) not in listed] if (root / "raw").exists() else []
    result["unlisted_raw_files"] = len(unlisted)
    result["counts"] = {
        "activities": con.execute("SELECT COUNT(*) FROM activities").fetchone()[0],
        "attempts": con.execute("SELECT COUNT(*) FROM attempts").fetchone()[0],
        "units": con.execute("SELECT COUNT(*) FROM units").fetchone()[0],
    }
    con.close()
    bad_parquet = 0
    parquet_files = list((root / "parquet").rglob("*.parquet")) if (root / "parquet").exists() \
        else []
    for path in parquet_files:
        try:
            pq.read_metadata(path)
        except Exception as exc:
            bad_parquet += 1
            problems.append(f"unreadable parquet {path.relative_to(root)}: {exc}")
    result.update(parquet_files=len(parquet_files), parquet_unreadable=bad_parquet)
    if smoke:
        result["smoke"] = smoke_test(root, ledger_path, problems)
    result["ok"] = not problems
    return result


def smoke_test(root: Path, ledger_path: Path, problems: list[str]) -> dict[str, Any]:
    """Run the MCP query layer against this copy of the archive."""
    from .analytics import Archive

    settings = dataclasses.replace(load_settings(), data_dir=root)
    if ledger_path != settings.ledger_path:
        problems.append("smoke test needs db/ledger.sqlite; restore a snapshot there first")
        return {}
    archive = Archive(settings)
    out: dict[str, Any] = {}
    try:
        cov = archive.data_coverage()
        out["backfill_complete"] = cov.get("report", {}).get("backfill_complete")
        if "activities" in archive.present:
            listed = archive.list_activities(limit=5)
            out["recent_activities"] = listed["count"]
            if listed["activities"]:
                key = listed["activities"][0]["activity_key"]
                stream = archive.get_activity_stream(key, max_points=50)
                out["stream_records"] = stream.get("total_records")
    except Exception as exc:
        problems.append(f"smoke test failed: {type(exc).__name__}: {exc}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.maintenance")
    sub = parser.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot")
    snap.add_argument("--keep", type=int, default=14)
    ver = sub.add_parser("verify")
    ver.add_argument("--root", type=Path)
    ver.add_argument("--ledger", type=Path)
    ver.add_argument("--smoke", action="store_true")
    args = parser.parse_args(argv)
    settings = load_settings()
    if args.command == "snapshot":
        print(snapshot(settings.data_dir, args.keep))
        return 0
    report = verify(args.root or settings.data_dir, args.ledger, args.smoke)
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
