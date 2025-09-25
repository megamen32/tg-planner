from typing import Iterable
import argparse
import random


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the ingest utility."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-c",
        "--cnt",
        required=True,
        help="Count of ids to generate",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    
    ids = [random.randint(800_000, 500_000_000) for _ in range(int(args.cnt))]
    with open("etl/files/ids_to_collect.txt", "w") as f:
        for i in ids:
            f.write(str(i) + "\n")
    print("Сгенерировано:", len(ids))
    
    return 0
    
if __name__ == "__main__":
    raise SystemExit(main())