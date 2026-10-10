# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+, tested against 7.0). All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_db   # or put them in .env (real env vars win)
python sample_seed.py            # optional: the nine sample documents
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). If MongoDB is not reachable at boot, `/health` returns 503 and creates them
on its first successful ping.

## Notes
- Instants are stored as BSON dates (UTC), returned as epoch milliseconds, and truncated to whole seconds (R1).
- Half-up rounding (R4, R8) uses `Decimal` in Python and `Decimal128` inside pipelines, because MongoDB's `$round` rounds
  half to even.
- Race safety comes from unique indexes (employee create, punch-in) and conditional updates (punch-out on
  `punch_out: null`; PATCH guarded by the old values and the history length, so the loser of a concurrent edit gets 409).
- The trend's `moving_avg_7d` averages the reported (4-decimal) `attendance_rate` values. A trend range may span at most
  92 days, counted inclusively.
- `REVIEW.md` lists the starter's defects; `DECISIONS.md` answers the design questions.
