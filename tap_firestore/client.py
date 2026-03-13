"""Base stream class for tap-firestore — reads incrementally from the `changes` collection."""

from datetime import datetime
from typing import Any, Iterable, Optional, Tuple, Union

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams import Stream

from tap_firestore.serializer import to_json_safe

PAGE_SIZE = 500


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
    ) -> None:
        super().__init__(name=name, schema=schema, tap=tap)
        self.db = db

    def get_records(
        self, context: Optional[dict]
    ) -> Iterable[Union[dict, Tuple[dict, dict]]]:
        from google.cloud.firestore_v1.base_query import FieldFilter

        tenant_id = self.config["tenant_id"]

        # Resolve bookmark: last synced value or start_date fallback
        start_value = self.get_starting_replication_key_value(context)
        if start_value is None:
            start_value = self.config.get("start_date")

        start_dt: Optional[datetime] = None
        if start_value:
            if isinstance(start_value, datetime):
                start_dt = start_value
            else:
                start_dt = datetime.fromisoformat(str(start_value).replace("Z", "+00:00"))

        query = (
            self.db.collection("changes")
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

                record = to_json_safe(data)
                record["received_at"] = to_json_safe(change.get("received_at"))
                record["action"] = change.get("action")
                yield record

            last_doc = docs[-1]
            if len(docs) < PAGE_SIZE:
                break
