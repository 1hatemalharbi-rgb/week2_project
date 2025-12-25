# (Week 2)

This project uses `uv` for dependency + virtual environment management.

## Requirements
- Python >= 3.11
- `uv` installed

## Setup (first time)
# Install Python (if not already present)
uv python install 3.11
# Install the virtual environment
winget install Astral.uv
### Windows (PowerShell)
```powershell
# 1) Create the virtual environment (optional if already created)run:
uv venv

# 2) Install dependencies from pyproject.toml (+ lockfile)run:
uv sync

# From the project root run:
$env:PYTHONPATH="src"
uv run python scripts\run_etl.py
### When the script finishes:

Output files will be created in:

data\processed\