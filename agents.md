# Development Notes

## Environment
- Requires Python 3.10+.
- Install dependencies with `python -m pip install -r requirements.txt` if the file appears in future updates. For now, only the standard library and `requests` are used.

## Quick checks
- Run `python -m compileall wb_client.py` to ensure the module has no syntax errors.
- Optional: execute a smoke request via `python - <<'PY'
from wildberries_client import get_card_api
print(get_card_api(139790298))
PY` when network access is permitted.

## Formatting & linting
- The project currently has no automated linters configured. Stick to PEP 8 and type hints using the Python 3.10+ union syntax.
