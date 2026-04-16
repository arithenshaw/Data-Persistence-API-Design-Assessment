# Name Profile API — Stage 1

FastAPI service that calls Genderize, Agify, and Nationalize, classifies the result, and persists it.

## Endpoints

- `POST /api/profiles` — create (idempotent by name)
- `GET /api/profiles` — list, filters: `gender`, `country_id`, `age_group`
- `GET /api/profiles/{id}`
- `DELETE /api/profiles/{id}`

## Run locally

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

Uses SQLite by default. Set `DATABASE_URL` for Postgres.
