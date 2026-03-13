"""Firestore type serializer — converts Firestore-specific types to JSON-safe values."""

import base64
from datetime import datetime


def to_json_safe(data):
    """Recursively convert Firestore types to JSON-serializable Python types."""
    if isinstance(data, dict):
        return {k: to_json_safe(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [to_json_safe(v) for v in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    elif hasattr(data, "latitude") and hasattr(data, "longitude"):
        return {"latitude": data.latitude, "longitude": data.longitude}
    elif hasattr(data, "path") and hasattr(data, "id") and hasattr(data, "parent"):
        # DocumentReference
        return data.path
    elif isinstance(data, bytes):
        return base64.b64encode(data).decode("utf-8")
    return data
