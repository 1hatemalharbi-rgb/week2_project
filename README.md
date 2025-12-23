## **Run Analytics Pipeline**

---

```bash
# Build analytics table
python scripts/run_day3_build_analytics.py

# Inspect output
python -c "import pandas as pd; df=pd.read_parquet('data/processed/analytics_table.parquet'); print(df.columns.tolist()); print(df[['user_id','country','month','amount','amount_winsor','amount_is_outlier']].head())"
