"""Base Firestore stream class used by the reusable extension package."""

import json
from datetime import datetime
from typing import Any, Iterable, Optional, Tuple, Union

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams import Stream

from tap_firestore.serializer import to_json_safe

PAGE_SIZE = 500


def _schema_type_includes(schema_property: dict, expected_type: str) -> bool:
    """Return True when a JSON Schema property accepts the expected type."""
    property_type = schema_property.get("type")
    if isinstance(property_type, str):
        return property_type == expected_type
    if isinstance(property_type, list):
        return expected_type in property_type
    return False


def _coerce_value_for_schema(value: Any, schema_property: dict) -> Any:
    """Coerce Firestore values to match schema shapes inherited from host taps."""
    if value is None:
        return None
    if _schema_type_includes(schema_property, "string") and not isinstance(value, str):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)
    return value


def coerce_record_to_schema(record: dict, schema: dict) -> dict:
    """Coerce known record fields to match their configured JSON Schema types."""
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    for field_name, schema_property in properties.items():
        if field_name in record and isinstance(schema_property, dict):
            record[field_name] = _coerce_value_for_schema(
                record[field_name],
                schema_property,
            )
    return record


class ChangesStream(Stream):
    """Base stream that reads incrementally from the Firestore `changes` collection.

    Subclasses must define `entity_type`, `primary_keys`, and `schema`.
    Each record is the contents of `payload.data` from a matching changes document,
    enriched with `received_at` (replication key) and `action` metadata fields.
    """

    entity_type: str = ""
    replication_key = "received_at"
    is_sorted = True

    def __init__(
        self,
        tap: TapBaseClass,
        db: Any,
        name: Optional[str] = None,
        schema=None,
        entity_name: Optional[str] = None,
        state_stream_name: Optional[str] = None,
        start_date: Optional[Any] = None,
        source_config: Optional[dict] = None,
        raw_payload: bool = False,
    ) -> None:
        super().__init__(name=name, schema=schema, tap=tap)
        self.db = db
        if entity_name:
            self.entity_type = entity_name
        self.state_stream_name = state_stream_name or self.name
        self.start_date = start_date
        self.raw_payload = raw_payload
        if source_config:
            self._config = {**dict(tap.config), **source_config}

    @property
    def stream_state(self) -> dict:
        """Return writable state using the configured bookmark namespace."""
        from singer_sdk.helpers._state import get_writeable_state_dict

        return get_writeable_state_dict(self.tap_state, self.state_stream_name)

    def get_context_state(self, context: Optional[dict]) -> dict:
        """Return writable state for the configured bookmark namespace."""
        state_partition_context = self._get_state_partition_context(context)
        if state_partition_context:
            from singer_sdk.helpers._state import get_writeable_state_dict

            return get_writeable_state_dict(
                self.tap_state,
                self.state_stream_name,
                state_partition_context=state_partition_context,
            )
        return self.stream_state

    def get_records(
        self, context: Optional[dict]
    ) -> Iterable[Union[dict, Tuple[dict, dict]]]:
        from google.cloud.firestore_v1.base_query import FieldFilter

        tenant_id = self.config["tenant_id"]
        collection_name = self.config.get("collection_name", "changes")

        # Resolve bookmark: last synced value or start_date fallback
        start_value = self.get_starting_replication_key_value(context)
        if start_value is None:
            start_value = self.start_date or self.config.get("start_date")

        start_dt: Optional[datetime] = None
        if start_value:
            if isinstance(start_value, datetime):
                start_dt = start_value
            else:
                start_dt = datetime.fromisoformat(
                    str(start_value).replace("Z", "+00:00")
                )

        self.logger.info(
            "Receiver query collection='%s' entity='%s' tenant='%s' start='%s' state_stream='%s'",
            collection_name,
            self.entity_type,
            tenant_id,
            start_dt.isoformat() if start_dt else None,
            self.state_stream_name,
        )

        query = (
            self.db.collection(collection_name)
            .where(filter=FieldFilter("tenant_id", "==", tenant_id))
            .where(filter=FieldFilter("entity_type", "==", self.entity_type))
            .order_by("received_at")
        )

        if start_dt:
            query = query.where(filter=FieldFilter("received_at", ">", start_dt))

        last_doc = None
        while True:
            page_query = query.limit(PAGE_SIZE)
            if last_doc is not None:
                page_query = page_query.start_after(last_doc)

            docs = list(page_query.stream())
            if not docs:
                break

            for doc in docs:
                change = doc.to_dict() or {}
                payload = change.get("payload")
                data = payload.get("data") if isinstance(payload, dict) else None

                if not data:
                    self.logger.debug(
                        "Skipping changes doc %s — no payload.data", doc.id
                    )
                    continue

                if self.raw_payload:
                    record = {
                        "entity_type": change.get("entity_type"),
                        "received_at": to_json_safe(change.get("received_at")),
                        "payload": to_json_safe(payload),
                    }
                else:
                    record = coerce_record_to_schema(to_json_safe(data), self.schema)
                    record["entity_type"] = change.get("entity_type")

                    schema_properties = set(self.schema.get("properties", {}).keys())
                    extra = {
                        k: to_json_safe(v)
                        for k, v in (data or {}).items()
                        if k not in schema_properties
                    }
                    record["extra_fields"] = json.dumps(extra) if extra else None

                record["received_at"] = to_json_safe(change.get("received_at"))
                record["action"] = change.get("action")
                yield record

            last_doc = docs[-1]
            if len(docs) < PAGE_SIZE:
                break
