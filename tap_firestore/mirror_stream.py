"""Generic Firestore mirror stream implementation."""

from __future__ import annotations

from typing import Any, Dict, List

from tap_firestore.client import ChangesStream


class FirestoreMirrorStream(ChangesStream):
    """Mirror a host tap stream using Firestore change events."""

    def __init__(
        self,
        tap,
        db: Any,
        *,
        name: str,
        entity_name: str,
        schema: Dict[str, Any],
        primary_keys: List[str],
        state_stream_name: str,
        start_date=None,
        source_config=None,
        raw_payload: bool = False,
    ) -> None:
        super().__init__(
            tap=tap,
            db=db,
            name=name,
            schema=schema,
            entity_name=entity_name,
            state_stream_name=state_stream_name,
            start_date=start_date,
            source_config=source_config,
            raw_payload=raw_payload,
        )
        self.primary_keys = primary_keys
        self.replication_key = "received_at"
