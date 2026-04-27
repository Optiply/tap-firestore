"""Helpers for deriving Firestore mirror schemas from host tap streams."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Optional


METADATA_PROPERTIES = {
    "entity_type": {"type": ["string", "null"]},
    "received_at": {"type": ["string", "null"], "format": "date-time"},
    "action": {"type": ["string", "null"]},
    "extra_fields": {"type": ["string", "null"]},
}


def merge_schema_with_extras(
    base_schema: Dict[str, Any],
    extra_properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Deep-copy a schema and add Firestore metadata fields."""
    schema = copy.deepcopy(base_schema)
    properties = copy.deepcopy(schema.get("properties", {}))
    properties.update(METADATA_PROPERTIES)
    if extra_properties:
        properties.update(copy.deepcopy(extra_properties))
    schema["type"] = "object"
    schema["properties"] = properties
    schema["additionalProperties"] = True
    return schema


def inherit_schema_from_stream(stream: Any) -> Dict[str, Any]:
    """Create a Firestore mirror schema from a host tap stream."""
    schema = getattr(stream, "schema", None)
    if not isinstance(schema, dict) or not schema:
        raise ValueError(f"Host stream '{stream.name}' does not expose a usable schema.")
    return merge_schema_with_extras(schema)


def load_schema_file(path: str) -> Dict[str, Any]:
    """Load a JSON schema file from disk."""
    schema_path = Path(path)
    if not schema_path.is_absolute():
        schema_path = Path.cwd() / schema_path
    return json.loads(schema_path.read_text())


def build_minimal_receiver_schema(
    entity_type: str,
    extra_properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a minimal receiver-native schema."""
    schema = {
        "type": "object",
        "properties": {
            "entity_type": {"type": ["string", "null"], "const": entity_type},
            "received_at": {"type": ["string", "null"], "format": "date-time"},
            "payload": {"type": ["object", "null"]},
        },
        "additionalProperties": True,
    }
    schema["properties"]["entity_type"] = {
        "type": ["string", "null"],
        "const": entity_type,
    }
    if extra_properties:
        schema["properties"].update(copy.deepcopy(extra_properties))
    return schema
