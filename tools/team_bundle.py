"""Build and verify the deterministic credential-free team-share bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterable


CONTRACT_PATH = Path("release/team_bundle_contract.json")
MANIFEST_NAME = "TEAM_BUNDLE_MANIFEST.json"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _git(repo_root: Path, *args: str, text: bool = True) -> str | bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=text,
    )
    return result.stdout


def load_contract(repo_root: Path) -> dict:
    path = repo_root / CONTRACT_PATH
    return json.loads(path.read_text(encoding="utf-8"))


def _normalise_path(path: str) -> str:
    normalised = PurePosixPath(path).as_posix()
    if normalised.startswith("/") or ".." in PurePosixPath(normalised).parts:
        raise ValueError(f"Unsafe archive path: {path}")
    return normalised


def _is_excluded(path: str, contract: dict) -> bool:
    path = _normalise_path(path)
    if path in set(contract.get("exclude_paths", [])):
        return True
    return any(path.startswith(prefix) for prefix in contract.get("exclude_prefixes", []))


def _assert_safe_path(path: str, contract: dict) -> None:
    posix = PurePosixPath(_normalise_path(path))
    lower_name = posix.name.lower()
    forbidden_names = {name.lower() for name in contract.get("forbidden_names", [])}
    forbidden_suffixes = tuple(suffix.lower() for suffix in contract.get("forbidden_suffixes", []))
    if lower_name in forbidden_names or lower_name.endswith(forbidden_suffixes):
        raise ValueError(f"Credential-like file is forbidden from the team bundle: {path}")


def _tracked_paths(repo_root: Path, revision: str, contract: dict) -> list[str]:
    raw = _git(repo_root, "ls-tree", "-r", "--name-only", "-z", revision, text=False)
    paths = [part.decode("utf-8") for part in raw.split(b"\0") if part]
    selected: list[str] = []
    for path in sorted(paths):
        if _is_excluded(path, contract):
            continue
        _assert_safe_path(path, contract)
        selected.append(_normalise_path(path))
    missing = sorted(set(contract["required_paths"]) - set(selected))
    if missing:
        raise ValueError("Required bundle members are absent from the revision: " + ", ".join(missing))
    return selected


def _read_git_blobs(repo_root: Path, revision: str, paths: Iterable[str]) -> Iterable[tuple[str, bytes]]:
    process = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        cwd=repo_root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    try:
        for path in paths:
            process.stdin.write(f"{revision}:{path}\n".encode("utf-8"))
            process.stdin.flush()
            header = process.stdout.readline().decode("ascii").strip().split()
            if len(header) != 3 or header[1] != "blob":
                raise RuntimeError(f"Unable to read committed blob for {path}: {' '.join(header)}")
            size = int(header[2])
            payload = process.stdout.read(size)
            delimiter = process.stdout.read(1)
            if len(payload) != size or delimiter != b"\n":
                raise RuntimeError(f"Truncated git blob for {path}")
            yield path, payload
    finally:
        process.stdin.close()
        return_code = process.wait()
        if return_code:
            stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
            raise RuntimeError(f"git cat-file failed with code {return_code}: {stderr}")


def _zip_info(name: str, timestamp: tuple[int, int, int, int, int, int]) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=timestamp)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = (0o100644 & 0xFFFF) << 16
    return info


def _commit_timestamp(repo_root: Path, revision: str) -> tuple[str, tuple[int, int, int, int, int, int]]:
    value = str(_git(repo_root, "show", "-s", "--format=%cI", revision)).strip()
    parsed = datetime.fromisoformat(value).astimezone(timezone.utc)
    # ZIP timestamps have a two-second resolution and no timezone field.
    second = parsed.second - parsed.second % 2
    return value, (parsed.year, parsed.month, parsed.day, parsed.hour, parsed.minute, second)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _archive_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_bundle(repo_root: Path, output: Path | None = None, revision: str = "HEAD") -> dict:
    repo_root = repo_root.resolve()
    contract = load_contract(repo_root)
    if str(_git(repo_root, "status", "--porcelain")).strip():
        raise RuntimeError("Refusing to build from a dirty working tree. Commit or stash changes first.")

    resolved_revision = str(_git(repo_root, "rev-parse", revision)).strip()
    branch = str(_git(repo_root, "rev-parse", "--abbrev-ref", revision)).strip()
    commit_time, zip_timestamp = _commit_timestamp(repo_root, resolved_revision)
    paths = _tracked_paths(repo_root, resolved_revision, contract)
    prefix = contract["archive_prefix"].rstrip("/") + "/"
    if output is None:
        filename = f"{contract['bundle_name']}-v{contract['bundle_version']}.zip"
        output = repo_root / "dist" / filename
    else:
        output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    members: list[dict] = []
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path, payload in _read_git_blobs(repo_root, resolved_revision, paths):
            archive.writestr(_zip_info(prefix + path, zip_timestamp), payload, compresslevel=9)
            members.append({"path": path, "bytes": len(payload), "sha256": _sha256(payload)})

        manifest = {
            "schema_version": "team-share-bundle/v1",
            "bundle_name": contract["bundle_name"],
            "bundle_version": contract["bundle_version"],
            "source_repository": contract["source_repository"],
            "source_revision": resolved_revision,
            "source_branch": branch,
            "source_commit_time": commit_time,
            "archive_prefix": contract["archive_prefix"],
            "member_count": len(members),
            "member_bytes": sum(member["bytes"] for member in members),
            "members": members,
            "required_paths": contract["required_paths"],
            "compact_reproduction_command": contract["compact_reproduction_command"],
            "full_data_runbook": contract["full_data_runbook"],
        }
        manifest_payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
        archive.writestr(
            _zip_info(prefix + MANIFEST_NAME, zip_timestamp), manifest_payload, compresslevel=9
        )

    archive_hash = _archive_sha256(output)
    sha_path = output.with_suffix(output.suffix + ".sha256")
    sha_path.write_text(f"{archive_hash}  {output.name}\n", encoding="utf-8", newline="\n")
    sidecar_path = output.with_suffix(output.suffix + ".manifest.json")
    sidecar = {
        **manifest,
        "archive_file": output.name,
        "archive_bytes": output.stat().st_size,
        "archive_sha256": archive_hash,
    }
    sidecar_path.write_text(
        json.dumps(sidecar, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return {"archive": str(output), "checksum": str(sha_path), "manifest": str(sidecar_path), **sidecar}


def _read_exact(handle: BinaryIO, expected_bytes: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    observed = 0
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        observed += len(block)
        digest.update(block)
    if observed != expected_bytes:
        raise ValueError(f"member byte mismatch: expected {expected_bytes}, observed {observed}")
    return digest.hexdigest(), observed


def verify_bundle(archive_path: Path, checksum_path: Path | None = None) -> list[str]:
    failures: list[str] = []
    archive_path = archive_path.resolve()
    if checksum_path is None:
        checksum_path = archive_path.with_suffix(archive_path.suffix + ".sha256")
    if checksum_path.exists():
        expected_archive_hash = checksum_path.read_text(encoding="utf-8").split()[0]
        observed_archive_hash = _archive_sha256(archive_path)
        if observed_archive_hash != expected_archive_hash:
            failures.append("archive SHA-256 mismatch")

    with zipfile.ZipFile(archive_path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            failures.append("archive contains duplicate member names")
        prefixes = {name.split("/", 1)[0] for name in names if "/" in name}
        if len(prefixes) != 1:
            failures.append(f"archive must have exactly one root directory, observed {sorted(prefixes)}")
            return failures
        prefix = next(iter(prefixes)) + "/"
        manifest_name = prefix + MANIFEST_NAME
        if manifest_name not in names:
            return failures + [f"missing internal {MANIFEST_NAME}"]
        manifest = json.loads(archive.read(manifest_name).decode("utf-8"))
        expected_names = {prefix + member["path"] for member in manifest["members"]}
        observed_names = set(names) - {manifest_name}
        missing = sorted(expected_names - observed_names)
        unexpected = sorted(observed_names - expected_names)
        if missing:
            failures.append("missing members: " + ", ".join(missing))
        if unexpected:
            failures.append("unexpected members: " + ", ".join(unexpected))
        for member in manifest["members"]:
            name = prefix + member["path"]
            if name not in observed_names:
                continue
            try:
                with archive.open(name) as handle:
                    observed_hash, _ = _read_exact(handle, member["bytes"])
                if observed_hash != member["sha256"]:
                    failures.append(f"member SHA-256 mismatch: {member['path']}")
            except (OSError, ValueError, zipfile.BadZipFile) as exc:
                failures.append(f"member verification failed for {member['path']}: {exc}")
        required = {prefix + path for path in manifest["required_paths"]}
        absent_required = sorted(required - observed_names)
        if absent_required:
            failures.append("required members absent: " + ", ".join(absent_required))
    return failures


def _default_archive(repo_root: Path) -> Path:
    contract = load_contract(repo_root)
    return repo_root / "dist" / f"{contract['bundle_name']}-v{contract['bundle_version']}.zip"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--repo-root", type=Path, default=_repo_root())
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--revision", default="HEAD")
    args = parser.parse_args()
    root = args.repo_root.resolve()
    archive = args.archive.resolve() if args.archive else _default_archive(root)
    if args.command == "build":
        print(json.dumps(build_bundle(root, archive, args.revision), indent=2))
        return
    failures = verify_bundle(archive)
    if failures:
        raise SystemExit("Team bundle verification failed:\n" + "\n".join(failures))
    print(f"Team bundle verification passed: {archive}")


if __name__ == "__main__":
    main()
