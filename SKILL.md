---
name: "openclaw-singapore-schools"
description: "Find primary and secondary schools near a Singapore address by asking for a 6-digit Singapore postal code and returning schools within 1 km and 2 km. Use when the user wants nearby-school checks in Singapore, school-radius lookups for a home address, or a quick list of primary and secondary schools around a postal code."
---

# Openclaw Singapore Schools

## Overview

Ask for a 6-digit Singapore postal code when the user does not provide one. Run the bundled helper to fetch the live MOE school directory, geocode the postal code and school postcodes with OneMap, and return primary and secondary schools within 1 km and 2 km.

## Workflow

1. Ask for the postal code if it is missing.
2. Run:

```bash
python3 /home/dreamtcs/.codex/skills/openclaw-singapore-schools/scripts/find_nearby_schools.py <postal-code>
```

3. Use `--format json` if you want machine-readable output before summarizing.
4. Report four groups clearly:
- Primary within 1 km
- Primary within 2 km
- Secondary within 1 km
- Secondary within 2 km

## Notes

- The helper uses official sources:
  - MOE school directory through `data.gov.sg`
  - Postal-code geocoding through OneMap
- OneMap's search endpoint may include an authentication warning string even when it still returns valid coordinates for postal-code search. The script accepts the result as long as coordinates are present.
- `MIXED LEVEL (P1-S4)` schools are included in both the primary and secondary groups.
- A local cache is stored under `~/.cache/openclaw-singapore-schools/` so repeat lookups are faster.

## Output Guidance

Keep the response compact and practical. Include the resolved input address, then list the schools sorted by distance within each radius bucket. If a bucket has no schools, say so explicitly.
