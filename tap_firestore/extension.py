"""Reusable Firestore extension for host Singer taps."""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List

from tap_firestore.firebase_auth import get_firestore_client
from tap_firestore.mirror_stream import FirestoreMirrorStream
from tap_firestore.schema_resolver import (
    build_minimal_receiver_schema,
    inherit_schema_from_stream,
    load_schema_file,
)


class FirestoreExtension:
    """Attach Firestore-backed mirror streams to a host tap."""

    REQUIRED_CONFIG_KEYS = (
        "tenant_uuid",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
    )

    def __init__(self, tap, config: Dict[str, Any]):
        self.tap = tap
        self.config = config
        self.db = None
        self.tenant = None

    def initialize(self) -> "FirestoreExtension":
        """Validate config, authenticate, and validate the tenant."""
        self._log_env_debug()
        self._validate_config()
        self.db = get_firestore_client(self.config)
        self.tenant = self._validate_tenant()
        return self

    def discover_streams(
        self, main_streams: Iterable[Any]
    ) -> List[FirestoreMirrorStream]:
        """Create receiver-backed streams for tap replacements and receiver-only streams."""
        main_stream_map = {stream.name: stream for stream in main_streams}
        receiver_streams: List[FirestoreMirrorStream] = []

        for stream_name, stream_config in self.get_tap_stream_configs().items():
            host_stream = main_stream_map.get(stream_name)
            if host_stream is None:
                raise ValueError(
                    f"Configured tap_stream '{stream_name}' was not found in host tap discovery."
                )
            primary_keys = stream_config.get("primary_keys") or list(
                host_stream.primary_keys
            )
            if not primary_keys:
                raise ValueError(
                    f"Host stream '{stream_name}' does not define primary keys required for receiver mirroring."
                )
            receiver_streams.append(
                FirestoreMirrorStream(
                    tap=self.tap,
                    db=self.db,
                    name=self.get_prefixed_state_name(stream_name),
                    entity_name=stream_config.get("entity_type", stream_name),
                    schema=self.resolve_tap_stream_schema(
                        stream_name, stream_config, host_stream
                    ),
                    primary_keys=primary_keys,
                    state_stream_name=self.get_prefixed_state_name(stream_name),
                    start_date=self.config.get("start_date"),
                    source_config=self.config,
                    raw_payload=False,
                )
            )

        for stream_name, stream_config in self.get_receiver_only_configs().items():
            receiver_streams.append(
                FirestoreMirrorStream(
                    tap=self.tap,
                    db=self.db,
                    name=stream_name,
                    entity_name=stream_config.get("entity_type", stream_name),
                    schema=self.resolve_receiver_only_schema(
                        stream_name, stream_config
                    ),
                    primary_keys=stream_config.get("primary_keys", []),
                    state_stream_name=self.get_prefixed_state_name(stream_name),
                    start_date=self.config.get("start_date"),
                    source_config=self.config,
                    raw_payload=True,
                )
            )

        return receiver_streams

    def filter_main_streams(self, main_streams: Iterable[Any]) -> List[Any]:
        """Keep host streams in discovery; runtime state decides which variant syncs."""
        stream_list = list(main_streams)
        stream_map = {stream.name: stream for stream in stream_list}
        for stream_name in self.get_tap_stream_configs():
            if stream_name not in stream_map:
                raise ValueError(
                    f"Configured tap_stream '{stream_name}' was not found in host tap discovery."
                )
        return stream_list

    def resolve_tap_stream_schema(
        self,
        stream_name: str,
        stream_config: Dict[str, Any],
        host_stream: Any,
    ) -> Dict[str, Any]:
        """Resolve the receiver schema for a host-backed stream."""
        schema_mode = stream_config.get("schema_mode", "inherit")
        if schema_mode == "inherit":
            return inherit_schema_from_stream(host_stream)
        if schema_mode == "file":
            schema_file = stream_config.get("schema_file")
            if not schema_file:
                raise ValueError(
                    f"Firestore stream '{stream_name}' uses schema_mode=file but no schema_file was provided."
                )
            return load_schema_file(schema_file)
        raise ValueError(
            f"Unsupported schema_mode '{schema_mode}' for Firestore stream '{stream_name}'."
        )

    def resolve_receiver_only_schema(
        self,
        stream_name: str,
        stream_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Resolve the schema for a receiver-native stream."""
        schema_mode = stream_config.get("schema_mode", "minimal")
        if schema_mode == "minimal":
            return build_minimal_receiver_schema(
                entity_type=stream_config.get("entity_type", stream_name),
                extra_properties=stream_config.get("extra_properties"),
            )
        if schema_mode == "file":
            schema_file = stream_config.get("schema_file")
            if not schema_file:
                raise ValueError(
                    f"receiver_only stream '{stream_name}' uses schema_mode=file but no schema_file was provided."
                )
            return load_schema_file(schema_file)
        raise ValueError(
            f"Unsupported schema_mode '{schema_mode}' for receiver_only stream '{stream_name}'."
        )

    def has_firestore_state(self, stream_name: str) -> bool:
        """Return True if receiver state exists for a stream."""
        bookmarks = self.tap.state.get("bookmarks", {})
        return self.get_prefixed_state_name(stream_name) in bookmarks

    @staticmethod
    def get_prefixed_state_name(stream_name: str) -> str:
        """Return the dedicated receiver state namespace for a host stream."""
        return f"receiver_{stream_name}"

    def should_use_receiver_tap_stream(self, stream_name: str) -> bool:
        """Return True when receiver state exists and host sync is not forced."""
        force_full_sync = self.tap.state.get("force_full_sync", [])
        return stream_name not in force_full_sync and self.has_firestore_state(
            stream_name
        )

    def apply_runtime_selection(self, streams_by_name: Dict[str, Any]) -> List[str]:
        """Select host or receiver variants at sync time. Returns names of streams doing full sync."""
        full_sync_streams: List[str] = []
        for stream_name in self.get_tap_stream_configs():
            host_stream = streams_by_name.get(stream_name)
            receiver_name = self.get_prefixed_state_name(stream_name)
            receiver_stream = streams_by_name.get(receiver_name)
            if host_stream is None or receiver_stream is None:
                continue
            if not host_stream.selected and not receiver_stream.selected:
                continue

            if self.should_use_receiver_tap_stream(stream_name):
                self._set_stream_selected(host_stream, False)
                self._set_stream_selected(receiver_stream, True)
                for child_stream in getattr(host_stream, "child_streams", []):
                    self._set_stream_selected(child_stream, False)
            else:
                self._set_stream_selected(host_stream, True)
                self._set_stream_selected(receiver_stream, False)
                full_sync_streams.append(stream_name)
        return full_sync_streams

    def write_post_full_sync_bookmarks(self, stream_names: List[str]) -> None:
        """After a full sync, write receiver bookmarks so the next run uses Firestore."""
        import json
        import sys

        configured = set(self.get_tap_stream_configs().keys())
        bookmarks = {}
        for stream_name in stream_names:
            if stream_name not in configured:
                continue
            receiver_name = self.get_prefixed_state_name(stream_name)
            bookmarks[receiver_name] = {
                "replication_key": "received_at",
                "replication_key_value": "",
            }
        if bookmarks:
            sys.stdout.write(
                json.dumps({"type": "STATE", "value": {"bookmarks": bookmarks}}) + "\n"
            )
            sys.stdout.flush()

    @staticmethod
    def _set_stream_selected(stream: Any, selected: bool) -> None:
        """Toggle a stream's root selection mask in place."""
        mask = getattr(stream, "mask", None)
        if isinstance(mask, dict):
            mask[()] = selected
            return
        if hasattr(stream, "_mask") and isinstance(stream._mask, dict):
            stream._mask[()] = selected
            return
        stream.selected = selected

    def _log_env_debug(self) -> None:
        """Log Firestore-related environment variables for Hotglue debugging."""
        explicit_keys = (
            "firestore_private_key",
            "FIRESTORE_PRIVATE_KEY",
            "TAP_FIRESTORE_PRIVATE_KEY",
            "TAP_FIRESTORE_CONFIG_PRIVATE_KEY",
        )
        firestore_env_keys = sorted(
            key for key in os.environ if "FIRESTORE" in key.upper()
        )
        keys_to_log = sorted({*explicit_keys, *firestore_env_keys})

        logger = getattr(self.tap, "logger", None)
        if logger is None:
            return

        logger.warning(
            "Firestore env debug: checking %s env var(s)",
            len(keys_to_log),
        )
        for key in keys_to_log:
            value = os.environ.get(key)
            if value is None:
                logger.warning("Firestore env debug: %s is NOT SET", key)
            else:
                logger.warning("Firestore env debug: %s=%s", key, value)

        logger.warning(
            "Firestore config debug: private_key=%s",
            self.config.get("private_key", "NOT SET"),
        )

    def _validate_config(self) -> None:
        missing_keys = [
            key for key in self.REQUIRED_CONFIG_KEYS if not self.config.get(key)
        ]
        if missing_keys:
            raise ValueError(
                "Missing required Firestore extension config values: "
                + ", ".join(sorted(missing_keys))
            )
        tap_streams = self.get_tap_stream_configs()
        receiver_only = self.get_receiver_only_configs()
        if not isinstance(tap_streams, dict):
            raise ValueError("Firestore extension 'tap_streams' must be an object.")
        if not isinstance(receiver_only, dict):
            raise ValueError("Firestore extension 'receiver_only' must be an object.")
        if not tap_streams and not receiver_only:
            raise ValueError(
                "Firestore extension requires at least one configured tap_stream or receiver_only stream."
            )

    def _validate_tenant(self) -> Dict[str, Any]:
        tenant_uuid = self.config["tenant_uuid"]
        tenant_doc = self.db.collection("tenants").document(tenant_uuid).get()
        if not tenant_doc.exists:
            raise ValueError(f"Tenant '{tenant_uuid}' not found in Firestore.")

        tenant = tenant_doc.to_dict() or {}
        status = tenant.get("status")
        if status != "active":
            raise ValueError(
                f"Tenant '{tenant_uuid}' has status '{status}'. Only active tenants can sync."
            )
        return tenant

    def _iter_enabled_stream_configs(self):
        for stream_name, stream_config in self.get_tap_stream_configs().items():
            yield stream_name, stream_config
        for stream_name, stream_config in self.get_receiver_only_configs().items():
            yield stream_name, stream_config

    def get_tap_stream_configs(self) -> Dict[str, Dict[str, Any]]:
        """Return normalized tap_stream configs."""
        return self._normalize_stream_mapping(
            self.config.get("tap_streams", {}), "tap_streams"
        )

    def get_receiver_only_configs(self) -> Dict[str, Dict[str, Any]]:
        """Return normalized receiver_only configs."""
        return self._normalize_stream_mapping(
            self.config.get("receiver_only", {}),
            "receiver_only",
        )

    @staticmethod
    def _normalize_stream_mapping(
        mapping: Any,
        section_name: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Normalize compact string or object stream configs into object configs."""
        if mapping is None:
            return {}
        if not isinstance(mapping, dict):
            raise ValueError(f"Firestore extension '{section_name}' must be an object.")

        normalized: Dict[str, Dict[str, Any]] = {}
        for stream_name, stream_config in mapping.items():
            if isinstance(stream_config, str):
                normalized[stream_name] = {"entity_type": stream_config}
                continue
            if isinstance(stream_config, dict):
                normalized[stream_name] = dict(stream_config)
                continue
            raise ValueError(
                f"Firestore extension '{section_name}.{stream_name}' must be a string or object."
            )
        return normalized
