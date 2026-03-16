#!/usr/bin/env python3
"""Find nearby Singapore primary and secondary schools from a postal code."""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


DATASET_ID = "d_688b934f82c1059ed0a6993d2a829089"
DATASET_ROWS_URL = (
    "https://api-production.data.gov.sg/v2/public/api/datasets/"
    f"{DATASET_ID}/list-rows"
)
DATASET_METADATA_URL = (
    "https://api-production.data.gov.sg/v2/public/api/datasets/"
    f"{DATASET_ID}/metadata"
)
ONEMAP_SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
DEFAULT_RADII_KM = (1.0, 2.0)
USER_AGENT = "codex-openclaw-singapore-schools/1.0"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
DEFAULT_MAX_WORKERS = 16


def cache_path() -> Path:
    base = Path.home() / ".cache" / "openclaw-singapore-schools"
    base.mkdir(parents=True, exist_ok=True)
    return base / "geocode-cache.json"


def fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return json.load(response)
        except Exception:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(0.5 * attempt)

    raise RuntimeError(f"Unreachable fetch retry loop for {url}")


def load_cache() -> dict[str, dict[str, float | str]]:
    path = cache_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def save_cache(cache: dict[str, dict[str, float | str]]) -> None:
    cache_path().write_text(json.dumps(cache, indent=2, sort_keys=True))


def normalise_postal_code(value: str) -> str:
    postal_code = "".join(ch for ch in value if ch.isdigit())
    if len(postal_code) != 6:
        raise ValueError("Singapore postal code must be 6 digits.")
    return postal_code


def normalise_dataset_postal_code(value: str) -> str | None:
    postal_code = "".join(ch for ch in value if ch.isdigit())
    if not postal_code:
        return None
    if len(postal_code) < 6:
        postal_code = postal_code.zfill(6)
    if len(postal_code) != 6:
        return None
    return postal_code


def geocode_postal_code(postal_code: str, cache: dict[str, dict[str, float | str]]) -> dict[str, Any]:
    if postal_code in cache:
        return cache[postal_code]

    query = urllib.parse.urlencode(
        {
            "searchVal": postal_code,
            "returnGeom": "Y",
            "getAddrDetails": "Y",
            "pageNum": 1,
        }
    )
    payload = fetch_json(f"{ONEMAP_SEARCH_URL}?{query}")
    results = payload.get("results") or []
    if not results:
        raise ValueError(f"Unable to geocode postal code {postal_code} via OneMap.")

    first = results[0]
    geocoded = {
        "postal_code": first.get("POSTAL", postal_code),
        "latitude": float(first["LATITUDE"]),
        "longitude": float(first["LONGITUDE"]),
        "address": (first.get("ADDRESS") or "").strip() or postal_code,
        "search_value": (first.get("SEARCHVAL") or "").strip(),
    }
    cache[postal_code] = geocoded
    return geocoded


def fetch_school_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    next_query = "limit=500"

    while next_query:
        payload = fetch_json(f"{DATASET_ROWS_URL}?{next_query}")
        data = payload.get("data") or {}
        rows.extend(data.get("rows") or [])
        next_query = ((data.get("links") or {}).get("next")) or ""

    return rows


def fetch_dataset_last_updated_at() -> str | None:
    payload = fetch_json(DATASET_METADATA_URL)
    data = payload.get("data") or {}
    return data.get("lastUpdatedAt")


def classify_school(row: dict[str, Any]) -> set[str]:
    mainlevel = (row.get("mainlevel_code") or "").upper()
    levels: set[str] = set()

    if mainlevel == "PRIMARY" or mainlevel == "MIXED LEVEL (P1-S4)":
        levels.add("primary")
    if "SECONDARY" in mainlevel or mainlevel == "MIXED LEVEL (P1-S4)":
        levels.add("secondary")

    return levels


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def school_result(
    row: dict[str, Any],
    geocoded: dict[str, Any],
    origin: dict[str, Any],
) -> dict[str, Any]:
    distance_km = haversine_km(
        origin["latitude"],
        origin["longitude"],
        geocoded["latitude"],
        geocoded["longitude"],
    )
    return {
        "school_name": row["school_name"].strip(),
        "postal_code": row["postal_code"],
        "address": row["address"].strip(),
        "mainlevel_code": row["mainlevel_code"],
        "type_code": row["type_code"],
        "distance_km": round(distance_km, 3),
        "latitude": geocoded["latitude"],
        "longitude": geocoded["longitude"],
    }


def build_results(
    postal_code: str,
    radii_km: list[float],
    max_workers: int,
) -> dict[str, Any]:
    cache = load_cache()
    origin = geocode_postal_code(postal_code, cache)
    school_rows = fetch_school_rows()

    relevant_rows = []
    for row in school_rows:
        levels = classify_school(row)
        school_postal = (row.get("postal_code") or "").strip()
        normalised_school_postal = normalise_dataset_postal_code(school_postal)
        if not levels or not normalised_school_postal:
            continue
        relevant_rows.append(
            {
                "row": row,
                "levels": levels,
                "postal_code": normalised_school_postal,
            }
        )

    unique_school_postals = sorted({item["postal_code"] for item in relevant_rows})
    missing_postals = [postal for postal in unique_school_postals if postal not in cache]

    if missing_postals:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(geocode_postal_code, postal, cache): postal
                for postal in missing_postals
            }
            for future in as_completed(futures):
                postal = futures[future]
                try:
                    future.result()
                except Exception:
                    cache.pop(postal, None)

    save_cache(cache)

    grouped: dict[str, list[dict[str, Any]]] = {"primary": [], "secondary": []}
    for item in relevant_rows:
        school_geocode = cache.get(item["postal_code"])
        if not school_geocode:
            continue

        result = school_result(item["row"], school_geocode, origin)
        if "primary" in item["levels"]:
            grouped["primary"].append(result)
        if "secondary" in item["levels"]:
            grouped["secondary"].append(result)

    for level in grouped:
        grouped[level].sort(key=lambda school: (school["distance_km"], school["school_name"]))

    within_radius: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for radius in radii_km:
        radius_key = f"{radius:g}km"
        within_radius[radius_key] = {}
        for level, schools in grouped.items():
            within_radius[radius_key][level] = [
                school for school in schools if school["distance_km"] <= radius
            ]

    return {
        "input_postal_code": postal_code,
        "input_location": origin,
        "radii_km": radii_km,
        "school_directory_last_updated_at": fetch_dataset_last_updated_at(),
        "results": within_radius,
    }


def format_school_line(school: dict[str, Any]) -> str:
    return (
        f"- {school['distance_km']:.3f} km | {school['school_name']} | "
        f"{school['postal_code']} | {school['address']} | {school['mainlevel_code']}"
    )


def to_markdown(payload: dict[str, Any]) -> str:
    lines = []
    origin = payload["input_location"]

    lines.append(f"# Nearby Schools For {payload['input_postal_code']}")
    lines.append("")
    lines.append(f"Resolved address: {origin['address']}")
    lines.append(
        "School directory last updated: "
        f"{payload.get('school_directory_last_updated_at') or 'unknown'}"
    )
    lines.append("")

    for radius in payload["results"]:
        lines.append(f"## Within {radius}")
        lines.append("")
        for level in ("primary", "secondary"):
            schools = payload["results"][radius][level]
            lines.append(f"### {level.capitalize()} ({len(schools)})")
            if schools:
                lines.extend(format_school_line(school) for school in schools)
            else:
                lines.append("- None")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find Singapore primary and secondary schools within 1 km and 2 km "
            "of a Singapore postal code."
        )
    )
    parser.add_argument("postal_code", help="6-digit Singapore postal code")
    parser.add_argument(
        "--radius",
        dest="radii_km",
        action="append",
        type=float,
        help="Radius in km. Repeat to return multiple cutoffs. Defaults to 1 and 2.",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Concurrent OneMap geocode requests while populating the local cache",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    try:
        radii_km = sorted(set(args.radii_km or DEFAULT_RADII_KM))
        postal_code = normalise_postal_code(args.postal_code)
        payload = build_results(
            postal_code=postal_code,
            radii_km=radii_km,
            max_workers=max(1, args.max_workers),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(to_markdown(payload), end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
