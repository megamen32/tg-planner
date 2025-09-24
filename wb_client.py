"""Compatibility wrapper around the Wildberries API client."""
from __future__ import annotations

import wildberries_client as _wildberries_client
from wildberries_client import *  # noqa: F401,F403 - re-exported for convenience

__all__ = getattr(_wildberries_client, "__all__", [])
