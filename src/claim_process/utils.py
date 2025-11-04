"""Utility functions for the application."""
import json
from decimal import Decimal
from uuid import UUID
from typing import Any


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal types."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def serialize_for_json(data: Any) -> Any:
    """
    Recursively convert Decimals to strings for JSON serialization.
    """
    if isinstance(data, dict):
        return {k: serialize_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_for_json(item) for item in data]
    elif isinstance(data, Decimal):
        return str(data)
    elif isinstance(data, UUID):
        return str(data)
    else:
        return data