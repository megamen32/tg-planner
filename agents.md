# Development Notes

## Repository overview
- The project now consists of two main modules: `wb_client.py` providing HTTP helpers for Wildberries endpoints and `wb_parser.py` offering higher-level normalization utilities.
- `__init__.py` currently exports nothing and simply marks the repository as a Python package root.

## Environment
- Requires Python 3.10 or newer.
- Third-party dependency: `requests` (install with `python -m pip install requests`). No additional requirements file is tracked yet.

## Quick checks
- Run `python -m compileall wb_client.py wb_parser.py` to ensure both modules pass a basic syntax check.
- Optional: lint manually with your preferred tool (e.g., `ruff`, `flake8`)—no automated linters are configured in the repo.

## Manual verification
- When network access is available you can sanity-check the HTTP client by calling:
  ```bash
  python - <<'PY'
  from wb_client import get_card_api, get_info_card_json, get_content_v2
  print(get_card_api(258368289).keys())
  print(bool(get_info_card_json(258368289)))
  print(bool(get_content_v2(258368289)))
  PY
  ```
- To validate the full normalization flow use the parser helper:
  ```bash
  python - <<'PY'
  from wb_parser import fetch_product_raw
  product = fetch_product_raw(258368289)
  print(product.to_dict())
  PY
  ```
  Inspect that the returned structure contains the expected Wildberries product data.

## Formatting & style
- Stick to PEP 8 conventions and prefer explicit type hints using Python 3.10+ union syntax (`int | None`).
