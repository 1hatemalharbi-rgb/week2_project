from pathlib import Path
from bootcamp_data.config import make_paths


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    paths = make_paths(root)

    print("week2:", paths.root)
    print("Raw data path:", paths.raw)
    print("Cache path:", paths.cache)
    print("Processed path:", paths.processed)
    print("External path:", paths.external)


if __name__ == "__main__":
    main()