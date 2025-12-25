# Week 2

This project uses `uv` for dependency + virtual environment management.

## Requirements
- Python >= 3.11
- `uv` installed

## Windows (PowerShell)

# 2) Install dependencies from pyproject.toml (+ lockfile)
uv sync

# From the project root run:
$env:PYTHONPATH="src"
uv run python scripts\run_etl.py

## When the script finishes

Output files will be created in:

data\processed\

## Run the notebook (VS Code)

Open the project folder in VS Code

Open:
notebooks\eda.ipynb

Choose a kernel, select the project environment:

.venv

Run all cells
