## Step 1 — Download the project

Open **PowerShell** and run:

```powershell`
git clone https://github.com/1hatemalharbi-rgb/week2_project.git
cd week2_project

This project uses `uv` for dependency + virtual environment management.

```## Requirements`
- Python >= 3.11
- `uv` installed
```### Windows (PowerShell)`
```powershell`
# 2) Install dependencies from pyproject.toml (+ lockfile)run:
uv sync

```# From the project root run:
$env:PYTHONPATH="src"
uv run python scripts\run_etl.py
### When the script finishes:

Output files will be created in:


data\processed\

### Run the notebook (VS Code)

Open the project folder in VS Code

Open notebooks\eda.ipynb

Choose a kernel, select the project environment:

.venv

Run all cells
