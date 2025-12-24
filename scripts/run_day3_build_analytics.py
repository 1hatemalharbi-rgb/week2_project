from pathlib import Path
import pandas as pd
import plotly.express as px
from bootcamp_data.config import make_paths
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize, add_outlier_flag
from bootcamp_data.joins import safe_left_join

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    p = make_paths(ROOT)
    orders = pd.read_parquet(p.processed / "orders_clean.parquet")

    users = pd.read_parquet(p.processed / "users.parquet")
    users = pd.concat([users, users.iloc[[0]]], ignore_index=True)

    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")

    assert_unique_key(users, "user_id")

    # time
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    n_missing_ts = int(orders_t["created_at"].isna().sum())
    print("missing created_at after parse:", n_missing_ts, "/", len(orders_t))

    # join (orders many -> users one)
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    assert len(joined) == len(orders_t), "Row count changed (join explosion?)"

    match_rate = 1.0 - float(joined["country"].isna().mean())
    print("rows:", len(joined))
    print("country match rate:", round(match_rate, 3))

    # outliers (keep rows; cap for viz)
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    # revenue summary by country
    summary = (
        joined.groupby("country", dropna=False)
        .agg(n=("order_id", "size"), revenue=("amount", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    print(summary)

    d = summary.sort_values("revenue", ascending=False)
    fig = px.bar(d, x="country", y="revenue", title="Revenue by country (All time)")
    fig.update_layout(title={"x": 0.02}, margin={"l": 60, "r": 20, "t": 60, "b": 60})
    fig.update_xaxes(title_text="Country")
    fig.update_yaxes(title_text="Revenue")
    fig

    # Optional: write summary CSV
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(reports_dir / "revenue_by_country.csv", index=False)

    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    print("wrote:", out_path)


if __name__ == "__main__":
    main()
