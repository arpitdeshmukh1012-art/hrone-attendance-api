"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file:
  1. Configuration and database
  2. Time and business-rule helpers (R1-R5, R8)
  3. Request models and validated parameter types
  4. Serialisation (MongoDB document -> API shape)
  5. Query / pipeline builders (shared by the endpoints and /admin/explain)
  6. Endpoints
"""
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import AfterValidator, BaseModel, Field, StringConstraints, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Configuration and database
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # real environment variables take priority over .env

log = logging.getLogger("attendance")

# tz_aware=True: PyMongo returns UTC-aware datetimes instead of naive ones, so
# arithmetic against IST-aware values is always correct.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
ABSENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_HOURS = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92
LIST_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]

_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: create_index is a no-op when an identical index already exists."""
    global _indexes_ready
    if _indexes_ready:
        return
    # emp_code is the business key; the unique index is what makes concurrent creates safe.
    employees.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    # GET /employees?department=..  (filter + sort by emp_code), trend's department match.
    employees.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="department_emp_code")
    # Department summary: employees with joined_on <= end of month (headcount, R9).
    employees.create_index([("joined_on", ASCENDING)], name="joined_on")
    # One record per (emp_code, date): punch-in race, PATCH lookup, per-employee month/range scans,
    # $lookup from employees, and punch-out's "latest record" search.
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_code_date")
    # GET /attendance sort (date desc, emp_code asc) and month-wide date-range scans (leaderboard).
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # do not crash the process; /health retries
        log.warning("index creation deferred: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time and business-rule helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: BSON dates are always UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_hhmm(value: str) -> time:
    h, m = map(int, value.split(":"))
    return time(h, m)


def is_overnight(emp: dict) -> bool:
    return parse_hhmm(emp["shift_end"]) <= parse_hhmm(emp["shift_start"])


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch belongs to the previous day."""
    ist = punch_in.astimezone(IST)
    d = ist.date()
    if is_overnight(emp) and ist.time() < parse_hhmm(emp["shift_end"]):
        d -= timedelta(days=1)
    return d


def shift_window(day: date, emp: dict) -> tuple[datetime, datetime]:
    """Shift start/end instants for the attendance day; an overnight shift ends on the next calendar day."""
    start = datetime.combine(day, parse_hhmm(emp["shift_start"]), IST)
    end = datetime.combine(day, parse_hhmm(emp["shift_end"]), IST)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def compute_late_minutes(punch_in: datetime, day: date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor minutes since shift start."""
    start, _ = shift_window(day, emp)
    seconds = int((punch_in - start).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_overtime(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: floor minutes after shift end, only when >= 30."""
    _, end = shift_window(day, emp)
    minutes = int((punch_out - end).total_seconds()) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def round_half_up(value: Any, places: int) -> Decimal:
    """R8: half-up on the decimal value (str() avoids binary-float artefacts such as 2.675 -> 2.67)."""
    if isinstance(value, Decimal128):
        value = value.to_decimal()
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> Decimal:
    """R4: seconds / 3600, 2 decimals half-up."""
    seconds = int((punch_out - punch_in).total_seconds())
    return round_half_up(Decimal(seconds) / Decimal(3600), 2)


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], day: date, emp: dict) -> dict:
    """The four API-computed fields for a record, per the table in DATA_MODEL.md section 3."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {"work_hours": None, "late_minutes": compute_late_minutes(punch_in, day, emp),
           "overtime_minutes": 0, "half_day": False}
    if punch_out is not None:
        hours = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = float(hours)
        out["overtime_minutes"] = compute_overtime(punch_out, day, emp)
        out["half_day"] = hours < HALF_DAY_HOURS  # R5 compares the rounded value
    return out


def month_bounds(month: str) -> tuple[date, date]:
    y, m = map(int, month.split("-"))
    first = date(y, m, 1)
    nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return first, nxt - timedelta(days=1)


def count_weekdays(start: date, end: date) -> int:
    if start > end:
        return 0
    days = (end - start).days + 1
    full_weeks, rest = divmod(days, 7)
    return full_weeks * 5 + sum(1 for i in range(rest) if (start + timedelta(days=i)).weekday() < 5)


def validation_error(loc: tuple, msg: str, value: Any = None) -> RequestValidationError:
    """422 with FastAPI's default body shape, for rules that cannot be expressed in the schema."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": value}])


# --------------------------------------------------------------------------- #
# 3. Request models and validated parameter types
# --------------------------------------------------------------------------- #
def _valid_date(v: str) -> str:
    date.fromisoformat(v)  # rejects 2026-02-30 etc.
    return v


DateStr = Annotated[str, StringConstraints(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), AfterValidator(_valid_date)]
MonthStr = Annotated[str, StringConstraints(pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")]
HHMM = Annotated[str, StringConstraints(pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")]
# Strict: floats, strings and booleans are rejected; the range rejects seconds-looking values.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
Limit = Annotated[int, Query(ge=1, le=50)]


class EmployeeCreate(BaseModel):
    emp_code: Annotated[str, StringConstraints(pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    email: Annotated[str, StringConstraints(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, StringConstraints(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: DateStr

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    # Derived fields (work_hours, late_minutes, ...) are not declared, so they are ignored if sent.
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: Annotated[str, StringConstraints(min_length=5, max_length=200)]
    regularized_by: Annotated[str, StringConstraints(min_length=1, max_length=50)]

    @model_validator(mode="before")
    @classmethod
    def _no_explicit_nulls(cls, data):
        # The contract types these as non-nullable; "omitted" means unchanged, null is not a value.
        if isinstance(data, dict):
            for key in ("status", "punch_in", "punch_out"):
                if key in data and data[key] is None:
                    raise ValueError(f"{key} may be omitted but not null")
        return data


class ExplainEndpoint(str, Enum):
    attendance_list = "attendance_list"
    employee_monthly = "employee_monthly"
    department_summary = "department_summary"
    late_leaderboard = "late_leaderboard"
    department_trend = "department_trend"


# --------------------------------------------------------------------------- #
# 4. Serialisation
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    created = doc.get("created_at") or doc["_id"].generation_time
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start", "09:30"),
        "shift_end": doc.get("shift_end", "18:30"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(created),
    }


def _api_value(v: Any) -> Any:
    return dt_to_ms(v) if isinstance(v, datetime) else v


def record_out(doc: dict) -> dict:
    """Missing late/overtime/half_day/history in legacy records mean 0/0/false/[] (DATA_MODEL.md)."""
    wh = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": None if wh is None else float(wh),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": [
            {
                "at": dt_to_ms(h["at"]),
                "by": h["by"],
                "reason": h["reason"],
                "changes": {k: {"from": _api_value(c.get("from")), "to": _api_value(c.get("to"))}
                            for k, c in (h.get("changes") or {}).items()},
            }
            for h in (doc.get("history") or [])
        ],
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 5. Query / pipeline builders - used by the endpoints AND by /admin/explain
# --------------------------------------------------------------------------- #
def _round_expr(expr: Any, places: int) -> dict:
    """Half-up rounding inside a pipeline. $round is half-to-even, so go through Decimal128:
    floor(x * 10^p + 0.5) / 10^p. Null propagates (stays null)."""
    scale = Decimal128(str(10 ** places))
    return {"$toDouble": {"$divide": [
        {"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, scale]}, Decimal128("0.5")]}},
        scale,
    ]}}


def _is_presence(prefix: str) -> dict:
    return {"$in": [f"{prefix}status", list(PRESENCE)]}


def _is_weekday(prefix: str) -> dict:
    d = {"$dateFromString": {"dateString": f"{prefix}date", "format": "%Y-%m-%d", "timezone": "UTC"}}
    return {"$lte": [{"$isoDayOfWeek": d}, 5]}


def _present_weight(prefix: str, weekdays_only: bool) -> dict:
    """R5/R6/R7: 1 for a presence record, 0.5 if half day, 0 otherwise."""
    cond = {"$and": [_is_presence(prefix), _is_weekday(prefix)]} if weekdays_only else _is_presence(prefix)
    return {"$cond": [cond, {"$cond": [{"$ifNull": [f"{prefix}half_day", False]}, 0.5, 1]}, 0]}


def _late(prefix: str) -> dict:
    return {"$ifNull": [f"{prefix}late_minutes", 0]}


def _is_late(prefix: str) -> dict:
    return {"$gt": [_late(prefix), 0]}


def attendance_list_query(emp_code, date_from, date_to, status) -> dict:
    if date_from and date_to and date_from > date_to:
        raise validation_error(("query", "date_from"), "date_from must be on or before date_to", date_from)
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    return q


def employee_monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": _present_weight("$", weekdays_only=True)},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [_is_late("$"), 1, 0]}},
            "total_late_minutes": {"$sum": _late("$")},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Starts from employees, so headcount includes people with zero logs (R9); logs join in via $lookup."""
    first, last = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    t = "$$this."
    per_log = lambda expr: {"$sum": {"$map": {"input": "$logs", "in": expr}}}  # noqa: E731
    has_hours = {"$and": [_is_presence(t), {"$isNumber": f"{t}work_hours"}]}
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "work_hours": 1, "late_minutes": 1, "half_day": 1}},
            ],
            "as": "logs",
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": per_log(_present_weight(t, weekdays_only=True))},
            # Decimal sum so the mean is rounded from the exact value, not a float approximation.
            "work_hours_sum": {"$sum": per_log({"$cond": [has_hours, {"$toDecimal": f"{t}work_hours"}, Decimal128("0")]})},
            "work_hours_n": {"$sum": per_log({"$cond": [has_hours, 1, 0]})},
            "late_count": {"$sum": per_log({"$cond": [_is_late(t), 1, 0]})},
            "total_late_minutes": {"$sum": per_log(_late(t))},
            "leave_count": {"$sum": per_log({"$cond": [{"$eq": [f"{t}status", "LEAVE"]}, 1, 0]})},
            "on_duty_count": {"$sum": per_log({"$cond": [{"$eq": [f"{t}status", "ON_DUTY"]}, 1, 0]})},
        }},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total": {"$sum": "$late_minutes"}, "count": {"$sum": 1}}},
        # Inner join: logs whose employee does not exist are dropped by the $unwind.
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp",
                     "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}]}},
        {"$unwind": "$emp"},
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # Standard competition ranking (1, 2, 2, 4); limit applies to the rank, not to the row count.
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": "$total", "late_count": "$count"}},
    ]
    return pipeline


def trend_range(from_: str, to: str) -> tuple[date, date]:
    start, end = date.fromisoformat(from_), date.fromisoformat(to)
    if end < start:
        raise validation_error(("query", "to"), "to must be on or after from", to)
    if (end - start).days + 1 > MAX_TREND_DAYS:
        raise validation_error(("query", "to"), f"range exceeds {MAX_TREND_DAYS} days", to)
    return start, end


def department_trend_pipeline(department: str, start: date, end: date) -> list:
    """One row per day in [start, end]. Each employee contributes one 'joined' event (at max(joined_on, start),
    or a zero event if they join after the range, so the stream is never empty) and one event per log.
    Events are grouped per day, gaps are filled with $densify, and window functions give the running
    headcount and the 7-row moving average."""
    s, e = start.isoformat(), end.isoformat()
    t = "$$this."
    lower = datetime.combine(start, time(), UTC)
    upper = datetime.combine(end + timedelta(days=1), time(), UTC)  # $densify upper bound is exclusive
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": s, "$lte": e}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
            ],
            "as": "logs",
        }},
        {"$project": {"_id": 0, "events": {"$concatArrays": [
            [{"$cond": [
                {"$lte": ["$joined_on", e]},
                {"date": {"$max": ["$joined_on", s]}, "joined": 1, "present": 0, "late": 0},
                {"date": s, "joined": 0, "present": 0, "late": 0},
            ]}],
            {"$map": {"input": "$logs", "in": {
                "date": f"{t}date", "joined": 0,
                "present": _present_weight(t, weekdays_only=False),
                "late": {"$cond": [_is_late(t), 1, 0]},
            }}},
        ]}}},
        {"$unwind": "$events"},
        {"$group": {"_id": "$events.date", "joined": {"$sum": "$events.joined"},
                    "present": {"$sum": "$events.present"}, "late": {"$sum": "$events.late"}}},
        {"$addFields": {"day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d", "timezone": "UTC"}}}},
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "headcount": {"$sum": "$joined", "window": {"documents": ["unbounded", "current"]}},
        }}},
        {"$addFields": {
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            "present_count": {"$ifNull": ["$present", 0]},
            "late_count": {"$ifNull": ["$late", 0]},
        }},
        {"$addFields": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            _round_expr({"$divide": [{"$toDecimal": "$present_count"}, "$headcount"]}, 4),
            None,
        ]}}},
        # $avg skips nulls and yields null when the window holds none; the window never sees rows before `from`.
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "moving_avg": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}},
        }}},
        {"$sort": {"day": 1}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d", "timezone": "UTC"}},
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": 1,
            "moving_avg_7d": _round_expr("$moving_avg", 4),
        }},
    ]


# --------------------------------------------------------------------------- #
# 6. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        ensure_indexes()
    except PyMongoError as exc:
        raise HTTPException(503, f"database unavailable: {exc.__class__.__name__}")
    return {"status": "ok"}


# ---- employees ------------------------------------------------------------ #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = body.model_dump()
    now = now_utc()
    doc["created_at"] = now.replace(microsecond=now.microsecond // 1000 * 1000)  # BSON dates hold ms
    try:
        employees.insert_one(doc)  # the unique index decides; no check-then-insert
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {} if department is None else {"department": department}
    cursor = employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": employees.count_documents(q),
            "page": page, "page_size": page_size}


# ---- attendance ----------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc().replace(microsecond=0)
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc().replace(microsecond=0)
    # A record's date is never after the IST date of its punch-in, so bounding `date` lets the
    # (emp_code, date) index walk backwards from today instead of scanning the employee's history.
    rec = logs.find_one(
        {"emp_code": body.emp_code, "date": {"$lte": ts.astimezone(IST).date().isoformat()}, "punch_in": {"$lte": ts}},
        sort=[("date", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    if ts <= rec["punch_in"] or ts - rec["punch_in"] > MAX_SHIFT:
        raise validation_error(("body", "punched_at"), "punched_at must be after punch_in and within 24 hours",
                               body.punched_at)
    day = date.fromisoformat(rec["date"])
    update = {"punch_out": ts, **derived_fields(rec["status"], rec["punch_in"], ts, day, emp)}
    # Conditional update: of two concurrent punch-outs only one still sees punch_out == null.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None}, {"$set": update}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    q = attendance_list_query(emp_code, date_from, date_to, status)
    cursor = logs.find(q).sort(LIST_SORT).skip((page - 1) * page_size).limit(page_size)
    items = [record_out(d) for d in cursor]
    total = logs.count_documents(q) if q else logs.estimated_document_count()
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: Annotated[DateStr, Path()], body: RegularizeRequest):
    emp = get_employee_or_404(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": date})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")
    day = datetime.strptime(date, "%Y-%m-%d").date()  # `date` is shadowed by the path parameter

    status = body.status or rec["status"]
    if status in ABSENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise validation_error(("body",), f"{status} clears punch times; do not supply punch_in/punch_out")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else rec.get("punch_in")
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else rec.get("punch_out")
        if new_in is None:
            raise validation_error(("body", "punch_in"), f"{status} requires a punch_in")
        if attendance_date(new_in, emp) != day:
            raise validation_error(("body", "punch_in"), "punch_in must stay on the record's attendance date",
                                   body.punch_in)
        if new_out is not None and (new_out <= new_in or new_out - new_in > MAX_SHIFT):
            raise validation_error(("body", "punch_out"), "punch_out must be after punch_in and within 24 hours",
                                   body.punch_out)

    new = {"status": status, "punch_in": new_in, "punch_out": new_out,
           **derived_fields(status, new_in, new_out, day, emp)}
    old = {
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day") or False),
    }
    changes = {k: {"from": old[k], "to": new[k]} for k in new if old[k] != new[k]}
    if not changes:
        raise validation_error(("body",), "the request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    # Optimistic concurrency: only apply if nobody changed the record since we read it. The history
    # length acts as a version number, so a concurrent PATCH can never overwrite another's entry.
    n = len(rec.get("history") or [])
    guard = {"_id": rec["_id"], "status": rec["status"], "punch_in": old["punch_in"], "punch_out": old["punch_out"],
             "history": {"$size": n} if n else {"$in": [None, []]}}
    updated = logs.find_one_and_update(guard, {"$set": new, "$push": {"history": entry}},
                                       return_document=ReturnDocument.AFTER)
    if updated is None:
        raise HTTPException(409, "record was modified concurrently; retry")
    return record_out(updated)


# ---- analytics ------------------------------------------------------------ #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Annotated[MonthStr, Query()]):
    emp = get_employee_or_404(emp_code)
    first, last = month_bounds(month)
    joined = date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(first, joined), last)  # R7: from joined_on for mid-month joiners
    row = next(logs.aggregate(employee_monthly_pipeline(emp_code, month)), None) or {}
    present = round_half_up(row.get("present_days", 0), 2)
    pct = None if working_days == 0 else float(round_half_up(present * 100 / working_days, 2))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": float(present),
        "leave_days": int(row.get("leave_days", 0)),
        "late_count": int(row.get("late_count", 0)),
        "total_late_minutes": int(row.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: Annotated[MonthStr, Query()], department: Optional[str] = None):
    items = []
    for row in employees.aggregate(department_summary_pipeline(month, department)):
        n = row["work_hours_n"]
        wh_sum = row["work_hours_sum"]
        wh_sum = wh_sum.to_decimal() if isinstance(wh_sum, Decimal128) else Decimal(str(wh_sum))
        items.append({
            "department": row["_id"],
            "headcount": int(row["headcount"]),
            "present_days": float(round_half_up(row["present_days"], 2)),
            "avg_work_hours": float(round_half_up(wh_sum / n, 2)) if n else None,
            "late_count": int(row["late_count"]),
            "total_late_minutes": int(row["total_late_minutes"]),
            "leave_count": int(row["leave_count"]),
            "on_duty_count": int(row["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: Annotated[MonthStr, Query()], limit: Limit = 10, department: Optional[str] = None):
    items = [
        {**row, "total_late_minutes": int(row["total_late_minutes"]), "late_count": int(row["late_count"])}
        for row in logs.aggregate(late_leaderboard_pipeline(month, limit, department))
    ]
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[DateStr, Query(alias="from")],
    to: Annotated[DateStr, Query()],
):
    start, end = trend_range(from_, to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    return {"department": department, "items": list(employees.aggregate(department_trend_pipeline(department, start, end)))}


# ---- admin ---------------------------------------------------------------- #
def _require(**params) -> None:
    for name, value in params.items():
        if value is None:
            raise RequestValidationError([{"type": "missing", "loc": ("query", name.rstrip("_")),
                                           "msg": "Field required", "input": None}])


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainEndpoint,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[MonthStr], Query()] = None,
    department: Optional[str] = None,
    limit: Limit = 10,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[DateStr], Query(alias="from")] = None,
    to: Optional[DateStr] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    if endpoint is ExplainEndpoint.attendance_list:
        collection = "attendance_logs"
        command = {"find": collection, "filter": attendance_list_query(emp_code, date_from, date_to, status),
                   "sort": dict(LIST_SORT), "skip": (page - 1) * page_size, "limit": page_size}
    else:
        if endpoint is ExplainEndpoint.employee_monthly:
            _require(emp_code=emp_code, month=month)
            collection, pipeline = "attendance_logs", employee_monthly_pipeline(emp_code, month)
        elif endpoint is ExplainEndpoint.department_summary:
            _require(month=month)
            collection, pipeline = "employees", department_summary_pipeline(month, department)
        elif endpoint is ExplainEndpoint.late_leaderboard:
            _require(month=month)
            collection, pipeline = "attendance_logs", late_leaderboard_pipeline(month, limit, department)
        else:
            _require(department=department, from_=from_, to=to)
            collection = "employees"
            pipeline = department_trend_pipeline(department, *trend_range(from_, to))
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    raw = db.command({"explain": command, "verbosity": "executionStats"})
    # Relaxed Extended JSON turns BSON-only types (Timestamp, ObjectId, Int64, ...) into plain JSON.
    return {"endpoint": endpoint.value, "collection": collection,
            "explain": json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))}
