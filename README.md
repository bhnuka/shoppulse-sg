# ShopPulse SG

Recruiter-ready monorepo for a Singapore registry intelligence platform.

## Stack
- Backend: FastAPI (Python)
- Frontend: Angular + TypeScript
- Database: ClickHouse
- Infra: Docker + Docker Compose

## Quick start (Docker)

```bash
cd infra
docker compose up --build
```

Services:
- Frontend: http://localhost:4200
- Backend: http://localhost:8000
- Health: http://localhost:8000/health
- ClickHouse: http://localhost:8123
- ClickHouse UI: http://localhost:5521

ClickHouse creds (default for docker-compose):
- user: default
- password: shoppulse

ClickHouse UI uses the same creds.

If the UI shows a connect screen, clear LocalStorage for http://localhost:5521 or open an Incognito window.


## Python deps (uv)

Install uv (one-time):
```bash
python -m pip install uv
```

Install runtime deps for scripts:
```bash
uv sync
```

Optional geo deps (for polygon matching):
```bash
uv sync --extra geo
```

## Data pipeline (Phase 2)

Generate cleaned ACRA CSV from local files (Phase 1):
```bash
python scripts/fetch_acra_collection.py --use-local --input-dir data/raw --out data/processed/acra_entities_cleaned.csv
```

Run the full ClickHouse pipeline (schema + dims + raw + enriched):
```bash
python scripts/run_pipeline.py --acra-csv data/processed/acra_entities_cleaned.csv --subzone-geojson data/map/MasterPlan2019SubzoneBoundaryNoSeaGEOJSON.geojson --planning-geojson data/map/MasterPlan2025PlanningAreaBoundaryNoSea.geojson --truncate --recreate
```

Optional geo enrichment (postal → lat/lon → subzone/planning area):
```bash
python scripts/run_pipeline.py --acra-csv data/processed/acra_entities_cleaned.csv --subzone-geojson data/map/MasterPlan2019SubzoneBoundaryNoSeaGEOJSON.geojson --planning-geojson data/map/MasterPlan2025PlanningAreaBoundaryNoSea.geojson --truncate --geo-enrich
```

## Repo structure
```
shoppulse-sg/
  backend/
  frontend/
  infra/
  docs/
  scripts/
  data/
```

## Notes
This is the Phase 2 scaffold. Backend endpoints, NL→SQL, and frontend analytics pages will be added in later phases.
