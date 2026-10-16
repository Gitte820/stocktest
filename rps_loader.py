from __future__ import annotations

import bisect
import hashlib
import pickle
import sqlite3
import struct
from pathlib import Path
from typing import Any


DEFAULT_RPS_EXTDATA_DIR = r"C:\Games\zd_zyb(x64 20250818 7.71)\T0002\extdata"

RPS_FILE_MAP = {
    1: "RPS120",
    2: "RPS250",
    3: "RPS10",
    4: "RPS20",
    5: "RPS50",
}

IDX_RECORD_SIZE = 29
DAT_RECORD_SIZE = 12
RPS_INDEX_CACHE_SCHEMA = 1
RPS_INDEX_CACHE_DIR = Path(".rps_cache")
RPS_INDEX_CACHE_PATH = RPS_INDEX_CACHE_DIR / "rps_index.sqlite3"
SQLITE_IN_CHUNK_SIZE = 800


def normalize_code(raw: Any) -> str:
    text = str(raw or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) > 6:
        digits = digits[-6:]
    return digits.zfill(6) if digits else ""


def rps_directory_status(extdata_dir: str | Path) -> dict[str, Any]:
    base = Path(str(extdata_dir or "")).expanduser()
    files = []
    missing = []

    for file_no, rps_name in RPS_FILE_MAP.items():
        idx_path = base / f"extdata_{file_no}.idx"
        dat_path = base / f"extdata_{file_no}.dat"
        idx_ok = idx_path.exists()
        dat_ok = dat_path.exists()
        if not idx_ok:
            missing.append(str(idx_path))
        if not dat_ok:
            missing.append(str(dat_path))
        files.append({
            "file_no": file_no,
            "name": rps_name,
            "idx_path": str(idx_path),
            "dat_path": str(dat_path),
            "idx_ok": idx_ok,
            "dat_ok": dat_ok,
            "idx_size": idx_path.stat().st_size if idx_ok else 0,
            "dat_size": dat_path.stat().st_size if dat_ok else 0,
        })

    return {
        "ok": base.exists() and not missing,
        "path": str(base),
        "missing": missing,
        "files": files,
    }


def rps_directory_signature(extdata_dir: str | Path) -> str:
    base = Path(str(extdata_dir or "")).expanduser()
    h = hashlib.sha1()
    h.update(str(base).encode("utf-8", errors="ignore"))

    for file_no in sorted(RPS_FILE_MAP):
        for suffix in ("idx", "dat"):
            path = base / f"extdata_{file_no}.{suffix}"
            h.update(str(path).encode("utf-8", errors="ignore"))
            if path.exists():
                stat = path.stat()
                h.update(str(stat.st_size).encode("ascii"))
                h.update(str(stat.st_mtime_ns).encode("ascii"))
            else:
                h.update(b"missing")

    return h.hexdigest()


def _parse_rps_pair(idx_path: Path, dat_path: Path) -> dict[str, list[tuple[str, float]]]:
    idx_bytes = idx_path.read_bytes()
    dat_bytes = dat_path.read_bytes()
    result: dict[str, list[tuple[str, float]]] = {}
    dat_offset = 0
    usable_idx_size = len(idx_bytes) // IDX_RECORD_SIZE * IDX_RECORD_SIZE

    for offset in range(0, usable_idx_size, IDX_RECORD_SIZE):
        rec = idx_bytes[offset: offset + IDX_RECORD_SIZE]
        code = normalize_code(rec[2:8].decode("ascii", errors="ignore"))
        count = struct.unpack_from("<I", rec, 25)[0]
        values: list[tuple[str, float]] = []

        for i in range(count):
            item_offset = dat_offset + i * DAT_RECORD_SIZE
            if item_offset + DAT_RECORD_SIZE > len(dat_bytes):
                break

            raw_date = struct.unpack_from("<I", dat_bytes, item_offset)[0]
            raw_value = struct.unpack_from("<f", dat_bytes, item_offset + 8)[0]
            values.append((str(raw_date), raw_value / 10.0))

        dat_offset += count * DAT_RECORD_SIZE

        if code and values:
            values.sort(key=lambda x: x[0])
            result[code] = values

    return result


def _parse_dat_series(dat_bytes: bytes, count: int) -> list[tuple[str, float]]:
    values: list[tuple[str, float]] = []

    for i in range(count):
        item_offset = i * DAT_RECORD_SIZE
        if item_offset + DAT_RECORD_SIZE > len(dat_bytes):
            break

        raw_date = struct.unpack_from("<I", dat_bytes, item_offset)[0]
        raw_value = struct.unpack_from("<f", dat_bytes, item_offset + 8)[0]
        values.append((str(raw_date), raw_value / 10.0))

    values.sort(key=lambda x: x[0])
    return values


def _parse_rps_pair_for_codes(
    idx_path: Path,
    dat_path: Path,
    wanted_codes: set[str],
) -> dict[str, list[tuple[str, float]]]:
    if not wanted_codes:
        return {}

    idx_bytes = idx_path.read_bytes()
    result: dict[str, list[tuple[str, float]]] = {}
    dat_offset = 0
    usable_idx_size = len(idx_bytes) // IDX_RECORD_SIZE * IDX_RECORD_SIZE

    with dat_path.open("rb") as dat_file:
        for offset in range(0, usable_idx_size, IDX_RECORD_SIZE):
            rec = idx_bytes[offset: offset + IDX_RECORD_SIZE]
            code = normalize_code(rec[2:8].decode("ascii", errors="ignore"))
            count = struct.unpack_from("<I", rec, 25)[0]
            byte_count = count * DAT_RECORD_SIZE

            if code in wanted_codes:
                dat_file.seek(dat_offset)
                values = _parse_dat_series(dat_file.read(byte_count), count)
                result[code] = values
                if len(result) >= len(wanted_codes):
                    break

            dat_offset += byte_count

    return result


def _connect_rps_index_cache() -> sqlite3.Connection:
    RPS_INDEX_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(RPS_INDEX_CACHE_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rps_series (
            schema_version INTEGER NOT NULL,
            signature TEXT NOT NULL,
            rps_name TEXT NOT NULL,
            code TEXT NOT NULL,
            series BLOB NOT NULL,
            PRIMARY KEY (schema_version, signature, rps_name, code)
        )
    """)
    return conn


def _chunked(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start: start + size]


def _read_cached_rps_series(
    conn: sqlite3.Connection,
    signature: str,
    rps_name: str,
    codes: list[str],
) -> dict[str, list[tuple[str, float]]]:
    cached: dict[str, list[tuple[str, float]]] = {}
    if not codes:
        return cached

    for chunk in _chunked(codes, SQLITE_IN_CHUNK_SIZE):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT code, series
            FROM rps_series
            WHERE schema_version = ?
              AND signature = ?
              AND rps_name = ?
              AND code IN ({placeholders})
            """,
            (RPS_INDEX_CACHE_SCHEMA, signature, rps_name, *chunk),
        ).fetchall()

        for code, blob in rows:
            try:
                series = pickle.loads(blob)
            except Exception:
                continue
            if isinstance(series, list):
                cached[normalize_code(code)] = series

    return cached


def _write_cached_rps_series(
    conn: sqlite3.Connection,
    signature: str,
    rps_name: str,
    series_map: dict[str, list[tuple[str, float]]],
) -> None:
    if not series_map:
        return

    rows = [
        (
            RPS_INDEX_CACHE_SCHEMA,
            signature,
            rps_name,
            normalize_code(code),
            sqlite3.Binary(pickle.dumps(series, protocol=pickle.HIGHEST_PROTOCOL)),
        )
        for code, series in series_map.items()
        if normalize_code(code)
    ]
    if rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO rps_series
                (schema_version, signature, rps_name, code, series)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )


def load_rps_directory_for_codes(
    extdata_dir: str | Path,
    codes: Any,
    signature: str | None = None,
) -> dict[str, dict[str, list[tuple[str, float]]]]:
    status = rps_directory_status(extdata_dir)
    if not status["ok"]:
        missing_text = "\n".join(status["missing"][:10])
        raise FileNotFoundError(f"RPS extdata 文件不完整：\n{missing_text}")

    raw_codes = [] if codes is None else codes
    code_list = sorted({normalize_code(code) for code in raw_codes if normalize_code(code)})
    data: dict[str, dict[str, list[tuple[str, float]]]] = {
        rps_name: {} for rps_name in RPS_FILE_MAP.values()
    }
    if not code_list:
        return data

    base = Path(str(extdata_dir or "")).expanduser()
    signature_text = str(signature or rps_directory_signature(base))
    conn = _connect_rps_index_cache()

    try:
        conn.execute(
            "DELETE FROM rps_series WHERE schema_version != ? OR signature != ?",
            (RPS_INDEX_CACHE_SCHEMA, signature_text),
        )

        for file_no, rps_name in RPS_FILE_MAP.items():
            cached = _read_cached_rps_series(conn, signature_text, rps_name, code_list)
            missing_codes = [code for code in code_list if code not in cached]

            if missing_codes:
                parsed = _parse_rps_pair_for_codes(
                    base / f"extdata_{file_no}.idx",
                    base / f"extdata_{file_no}.dat",
                    set(missing_codes),
                )
                missing_series = {
                    code: parsed.get(code, [])
                    for code in missing_codes
                }
                _write_cached_rps_series(conn, signature_text, rps_name, missing_series)
                cached.update(missing_series)

            data[rps_name] = cached

        conn.commit()
    finally:
        conn.close()

    return data


def clear_rps_index_cache() -> None:
    try:
        if RPS_INDEX_CACHE_PATH.exists():
            RPS_INDEX_CACHE_PATH.unlink()
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{RPS_INDEX_CACHE_PATH}{suffix}")
            if sidecar.exists():
                sidecar.unlink()
    except Exception:
        try:
            conn = _connect_rps_index_cache()
            conn.execute("DELETE FROM rps_series")
            conn.commit()
            conn.close()
        except Exception:
            pass


def load_rps_directory(extdata_dir: str | Path) -> dict[str, dict[str, list[tuple[str, float]]]]:
    status = rps_directory_status(extdata_dir)
    if not status["ok"]:
        missing_text = "\n".join(status["missing"][:10])
        raise FileNotFoundError(f"RPS extdata 文件不完整：\n{missing_text}")

    base = Path(str(extdata_dir or "")).expanduser()
    data: dict[str, dict[str, list[tuple[str, float]]]] = {}

    for file_no, rps_name in RPS_FILE_MAP.items():
        data[rps_name] = _parse_rps_pair(
            base / f"extdata_{file_no}.idx",
            base / f"extdata_{file_no}.dat",
        )

    return data


def lookup_rps_values(
    rps_data: dict[str, dict[str, list[tuple[str, float]]]],
    code: Any,
    date_text: Any,
) -> dict[str, float | None]:
    code_text = normalize_code(code)
    key_date = str(date_text or "").replace("-", "")[:8]
    values: dict[str, float | None] = {}

    for rps_name in RPS_FILE_MAP.values():
        series = (rps_data or {}).get(rps_name, {}).get(code_text)
        if not series:
            values[rps_name] = None
            continue

        dates = [item[0] for item in series]
        idx = bisect.bisect_right(dates, key_date) - 1
        values[rps_name] = series[idx][1] if idx >= 0 else None

    return values
