"""Generic Firestore tap for direct schema-file based usage."""

from typing import Dict, List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_firestore.extension import FirestoreExtension


class TapFirestore(Tap):
    """Direct Firestore tap wrapper using explicit stream config."""

    name = "tap-firestore"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "tenant_uuid",
            th.StringType,
            required=True,
            description="Firestore document ID/UUID of the tenant to sync.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest received_at value to sync on first run.",
        ),
        # Firebase service account — only fields actually used by google-auth
        th.Property("project_id", th.StringType, required=False),
        th.Property("private_key_id", th.StringType, required=False),
        th.Property("private_key", th.StringType, required=False),
        th.Property("client_email", th.StringType, required=False),
        th.Property("firestore_project_id", th.StringType, required=False),
        th.Property("firestore_private_key_id", th.StringType, required=False),
        th.Property("firestore_private_key", th.StringType, required=False),
        th.Property("firestore_client_email", th.StringType, required=False),
        th.Property(
            "token_uri",
            th.StringType,
            default="https://oauth2.googleapis.com/token",
        ),
        th.Property(
            "collection_name",
            th.StringType,
            default="changes",
            description="Top-level Firestore collection holding receiver change documents.",
        ),
        th.Property(
            "tap_streams",
            th.CustomType(
                {
                    "type": "object",
                    "additionalProperties": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "object",
                                "properties": {
                                    "entity_type": {"type": "string"},
                                    "collection_name": {"type": "string"},
                                    "schema_mode": {
                                        "type": "string",
                                        "enum": ["inherit", "file"],
                                    },
                                    "schema_file": {"type": "string"},
                                    "primary_keys": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                                "additionalProperties": False,
                            },
                        ]
                    },
                }
            ),
            required=False,
        ),
        th.Property(
            "receiver_only",
            th.CustomType(
                {
                    "type": "object",
                    "additionalProperties": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "object",
                                "properties": {
                                    "entity_type": {"type": "string"},
                                    "schema_mode": {
                                        "type": "string",
                                        "enum": ["minimal", "file"],
                                    },
                                    "schema_file": {"type": "string"},
                                    "primary_keys": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                    "extra_properties": {"type": "object"},
                                },
                                "additionalProperties": False,
                            },
                        ]
                    },
                }
            ),
            required=False,
        ),
    ).to_dict()

    def load_state(self, state):
        """Preserve receiver extension state flags in addition to bookmarks."""
        super().load_state(state)
        if "force_full_sync" in state:
            self.state["force_full_sync"] = list(state.get("force_full_sync", []))

    def discover_streams(self) -> List[Stream]:
        """Discover streams using explicit schema-file config."""
        host_streams = [
            _ConfiguredHostStream(
                tap=self,
                name=stream_name,
                schema_file=stream_config["schema_file"],
                primary_keys=stream_config.get("primary_keys", []),
            )
            for stream_name, stream_config in FirestoreExtension(
                tap=self,
                config=dict(self.config),
            ).get_tap_stream_configs().items()
            if "schema_file" in stream_config
        ]
        extension = FirestoreExtension(tap=self, config=dict(self.config)).initialize()
        return extension.discover_streams(host_streams)


class _ConfiguredHostStream(Stream):
    """Minimal host stream wrapper for direct tap-firestore usage."""

    def __init__(
        self,
        tap: Tap,
        *,
        name: str,
        schema_file: str,
        primary_keys: List[str],
    ) -> None:
        super().__init__(tap=tap, name=name, schema=schema_file)
        self.primary_keys = primary_keys


if __name__ == "__main__":
    TapFirestore.cli()
