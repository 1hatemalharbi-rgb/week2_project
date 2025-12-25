import re
import pandas as pd

from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.joins import safe_left_join

# --- HELPER FUNCTIONS (Restored to fix "not defined" errors) ---

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

_ws = re.compile(r"\s+")
def normalize_text(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.casefold().str.replace(_ws, " ", regex=True)

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    x = df[col].dropna()
    q1, q3 = x.quantile(0.25), x.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - k * iqr, q3 + k * iqr
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})

# --- MAIN TRANSFORM ---

def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    # Fail-fast checks (before doing work)
    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    # The “one” side must be unique for a many→one join
    assert_unique_key(users, "user_id")

    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}

    orders = (
        orders_raw.pipe(enforce_schema)
        .assign(
            status_clean=lambda d: apply_mapping(normalize_text(d["status"]), status_map)
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # Left join should not change row count
    assert len(joined) == len(orders), "Row count changed (join explosion?)"

    # Outliers: keep raw `amount`, add winsorized + outlier flag for analysis
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    return joined