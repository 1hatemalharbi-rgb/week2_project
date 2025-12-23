Run the analytics pipeline to create the final analytics dataset and inspect the output.

▶️ Step 1: Build the analytics table
python scripts/run_day3_build_analytics.py


This script:

Loads cleaned orders and users data

Adds time-based features

Safely joins orders to users

Winsorises order amounts and flags outliers

Writes the final analytics table to:

data/processed/analytics_table.parquet

🔍 Step 2: Inspect the output

Use the following command to load the analytics table and preview key columns:

python -c "import pandas as pd; df = pd.read_parquet('data/processed/analytics_table.parquet'); print(df.columns.tolist()); print(df[['user_id','country','month','amount','amount_winsor','amount_is_outlier']].head())"


This will:

Print all column names in the analytics table

Display the first few rows for selected variables, including:

user_id

country

month

amount

amount_winsor

amount_is_outlier
