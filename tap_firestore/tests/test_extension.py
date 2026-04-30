"""Unit tests for the reusable Firestore extension."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_firestore.extension import FirestoreExtension
from tap_firestore.mirror_stream import FirestoreMirrorStream
from tap_firestore.schema_resolver import (
    build_minimal_receiver_schema,
    inherit_schema_from_stream,
    load_schema_file,
)


class DummyTap(Tap):
    name = "dummy-tap"
    config_jsonschema = th.PropertiesList().to_dict()

    def load_state(self, state):
        super().load_state(state)
        if "force_full_sync" in state:
            self.state["force_full_sync"] = list(state.get("force_full_sync", []))

    def discover_streams(self):
        return []


class OrdersHostStream(Stream):
    name = "orders"
    primary_keys = ["idorder"]
    schema = th.PropertiesList(
        th.Property("idorder", th.IntegerType),
        th.Property("created", th.DateTimeType),
    ).to_dict()

    def get_records(self, context):
        return iter(())


class ProductsHostStream(Stream):
    name = "products"
    primary_keys = ["idproduct"]
    schema = th.PropertiesList(
        th.Property("idproduct", th.IntegerType),
    ).to_dict()

    def get_records(self, context):
        return iter(())


class FakeDoc:
    def __init__(self, exists=True, data=None):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class FakeTenantDocument:
    def __init__(self, tenant):
        self.tenant = tenant

    def get(self):
        return FakeDoc(True, self.tenant)


class FakeCollection:
    def __init__(self, tenant):
        self.tenant = tenant

    def document(self, tenant_id):
        return FakeTenantDocument(self.tenant)


class FakeDb:
    def __init__(self, tenant=None):
        self.tenant = tenant or {"status": "active"}

    def collection(self, name):
        assert name == "tenants"
        return FakeCollection(self.tenant)


def build_extension(state=None):
    tap = DummyTap(config={}, state=state or {})
    extension = FirestoreExtension(
        tap=tap,
        config={
            "tenant_id": "tenant-1",
            "project_id": "project-id",
            "private_key_id": "key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
            "client_email": "test@example.com",
            "collection_name": "picqer_changes",
            "tap_streams": {
                "orders": "orders",
                "products": "products",
            },
            "receiver_only": {
                "deleted_orders": {
                    "entity_type": "orders",
                    "schema_mode": "minimal",
                    "extra_properties": {
                        "source": {"type": ["string", "null"]},
                    },
                }
            },
        },
    )
    extension.db = FakeDb()
    extension.tenant = {"status": "active"}
    return tap, extension


def test_inherit_schema_adds_receiver_metadata():
    tap = DummyTap(config={})
    schema = inherit_schema_from_stream(OrdersHostStream(tap=tap))
    assert "entity_type" in schema["properties"]
    assert "received_at" in schema["properties"]
    assert "action" in schema["properties"]
    assert schema["additionalProperties"] is True


def test_load_schema_file(tmp_path: Path):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps({"type": "object", "properties": {"id": {"type": "integer"}}}))
    assert load_schema_file(str(schema_path))["properties"]["id"]["type"] == "integer"


def test_build_minimal_receiver_schema():
    schema = build_minimal_receiver_schema(
        entity_type="orders",
        extra_properties={"source": {"type": ["string", "null"]}},
    )
    assert schema["properties"]["entity_type"]["const"] == "orders"
    assert "received_at" in schema["properties"]
    assert "payload" in schema["properties"]
    assert "source" in schema["properties"]


def test_first_run_uses_receiver_for_tap_streams():
    _, extension = build_extension()
    tap = extension.tap
    main_streams = [OrdersHostStream(tap=tap), ProductsHostStream(tap=tap)]

    filtered = extension.filter_main_streams(main_streams)
    assert [stream.name for stream in filtered] == ["orders", "products"]

    discovered = extension.discover_streams(main_streams)
    assert [stream.name for stream in discovered] == [
        "receiver_orders",
        "receiver_products",
        "deleted_orders",
    ]
    assert discovered[0].state_stream_name == "receiver_orders"
    assert discovered[0].schema["properties"]["received_at"]["format"] == "date-time"
    assert discovered[2].state_stream_name == "receiver_deleted_orders"
    assert discovered[2].raw_payload is True


def test_receiver_runs_even_without_receiver_state():
    state = {}
    tap, extension = build_extension(state=state)
    main_streams = [OrdersHostStream(tap=tap), ProductsHostStream(tap=tap)]

    filtered = extension.filter_main_streams(main_streams)
    assert [stream.name for stream in filtered] == ["orders", "products"]

    discovered = extension.discover_streams(main_streams)
    assert [stream.name for stream in discovered] == [
        "receiver_orders",
        "receiver_products",
        "deleted_orders",
    ]
    assert discovered[0].config["collection_name"] == "picqer_changes"


def test_string_mapping_is_normalized_for_tap_and_receiver_only():
    _, extension = build_extension()
    assert extension.get_tap_stream_configs()["orders"]["entity_type"] == "orders"
    assert extension.get_tap_stream_configs()["products"]["entity_type"] == "products"

    extension.config["receiver_only"] = {"deleted_orders": "orders_deleted"}
    assert extension.get_receiver_only_configs()["deleted_orders"]["entity_type"] == "orders_deleted"


def test_force_full_sync_disables_tap_stream_receiver():
    state = {"force_full_sync": ["orders"]}
    tap, extension = build_extension(state=state)
    main_streams = [OrdersHostStream(tap=tap), ProductsHostStream(tap=tap)]

    filtered = extension.filter_main_streams(main_streams)
    assert [stream.name for stream in filtered] == ["orders", "products"]
    assert not hasattr(filtered[0], "state_stream_name")

    discovered = extension.discover_streams(main_streams)
    assert [stream.name for stream in discovered] == [
        "receiver_orders",
        "receiver_products",
        "deleted_orders",
    ]


def test_runtime_selection_force_full_sync_activates_host():
    _, extension = build_extension(state={"force_full_sync": ["orders"]})
    parent = SimpleNamespace(selected=False, child_streams=[])
    receiver = SimpleNamespace(selected=True)

    full_sync_streams = extension.apply_runtime_selection(
        {"orders": parent, "receiver_orders": receiver},
    )

    assert parent.selected is True
    assert receiver.selected is False
    assert full_sync_streams == ["orders"]


def test_runtime_selection_activates_receiver_and_disables_host_children():
    _, extension = build_extension()
    child = SimpleNamespace(selected=True)
    parent = SimpleNamespace(selected=True, child_streams=[child])
    receiver = SimpleNamespace(selected=False)

    extension.apply_runtime_selection(
        {"orders": parent, "receiver_orders": receiver},
    )
    assert parent.selected is False
    assert receiver.selected is True
    assert child.selected is False
