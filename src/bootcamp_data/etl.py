from __future__ import annotations
import json
import pandas as pd
import logging
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

# Connects to the function we just added to transforms.py
from bootcamp_data.transforms import transform 
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet

@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

log = logging.getLogger(__name__)

# CODE 1: load_outputs
def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """Write processed artifacts (idempotent)."""
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
    ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)

# CODE 2: write_run_meta
def write_run_meta(
    cfg: ETLConfig, *, orders_raw: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame
) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )

    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")

# CODE 3: run_etl
def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics=analytics, users=users, cfg=cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, orders_raw=orders_raw, users=users, analytics=analytics)