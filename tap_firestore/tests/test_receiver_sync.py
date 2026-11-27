"""Exercise receiver records and checkpoints through the installed SDK."""

import json
from datetime import datetime, timezone
from unittest.mock import Mock

from hotglue_singer_sdk import Tap

from tap_firestore.mirror_stream import FirestoreMirrorStream


class ReceiverTap(Tap):
    """Credential-free host for the real receiver stream."""

    name = "tap-receiver-test"
    config_jsonschema = {"type": "object", "properties": {}}

    def discover_streams(self):
        """Supply streams explicitly in the test."""
        return []


def test_receiver_emits_records_and_resumes_existing_bookmark(capsys):
    """Keep receiver namespaces and query strictly after the last emitted event."""
    cursor = datetime(2026, 1, 1, tzinfo=timezone.utc)
    state = {
        "bookmarks": {
            "receiver_orders": {
                "replication_key": "received_at",
                "replication_key_value": cursor.isoformat(),
            }
        }
    }
    for day in (2, 3):
        received_at = datetime(2026, 1, day, tzinfo=timezone.utc)
        doc = Mock()
        doc.to_dict.return_value = {
            "tenant_uuid": "synthetic-tenant",
            "entity_type": "orders",
            "received_at": received_at,
            "action": "updated",
            "payload": {"data": {"idorder": day}},
        }
        query = Mock()
        query.where.return_value = query
        query.order_by.return_value = query
        query.limit.return_value = query
        query.stream.return_value = iter([doc])
        db = Mock()
        db.collection.return_value = query
        tap = ReceiverTap(config={"tenant_uuid": "synthetic-tenant"}, state=state)
        stream = FirestoreMirrorStream(
            tap=tap,
            db=db,
            name="receiver_orders",
            entity_name="orders",
            state_stream_name="receiver_orders",
            primary_keys=["idorder"],
            schema={
                "type": "object",
                "properties": {
                    "idorder": {"type": "integer"},
                    "received_at": {"type": "string", "format": "date-time"},
                },
            },
        )
        stream.sync()
        stream.finalize_state_progress_markers()
        messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        records = [
            message["record"] for message in messages if message["type"] == "RECORD"
        ]
        assert [record["idorder"] for record in records] == [day]
        filters = [call.kwargs["filter"] for call in query.where.call_args_list]
        assert [(item.field_path, item.op_string, item.value) for item in filters] == [
            ("tenant_uuid", "==", "synthetic-tenant"),
            ("entity_type", "==", "orders"),
            ("received_at", ">", cursor),
        ]
        assert set(tap.state["bookmarks"]) == {"receiver_orders"}
        bookmark = tap.state["bookmarks"]["receiver_orders"]
        assert bookmark["replication_key"] == "received_at"
        cursor = datetime.fromisoformat(
            bookmark["replication_key_value"].replace("Z", "+00:00")
        )
        assert cursor == received_at
        assert any(message["type"] == "STATE" for message in messages)
        state = json.loads(json.dumps(tap.state))
