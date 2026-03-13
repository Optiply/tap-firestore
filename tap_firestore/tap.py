"""Firestore tap class."""

from typing import List

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore as fb_firestore
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_firestore.streams import (
    OrdersStream,
    ProductsStream,
    PurchaseOrdersStream,
    ReceiptsStream,
    SuppliersStream,
)

# Maps entity names (from the tenant's `entities` list) to stream classes.
ENTITY_STREAM_MAP = {
    "products": ProductsStream,
    "orders": OrdersStream,
    "purchase_orders": PurchaseOrdersStream,
    "receipts": ReceiptsStream,
    "suppliers": SuppliersStream,
}


class TapFirestore(Tap):
    """Firestore tap — reads change events for a given tenant."""

    name = "tap-firestore"

    config_jsonschema = th.PropertiesList(
        # Tenant
        th.Property(
            "tenant_id",
            th.StringType,
            required=True,
            description="Firestore document ID of the tenant to sync.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest received_at value to sync on first run.",
        ),
        # Firebase service account — only fields actually used by google-auth
        th.Property("project_id", th.StringType, required=True),
        th.Property("private_key_id", th.StringType, required=True),
        th.Property("private_key", th.StringType, required=True),
        th.Property("client_email", th.StringType, required=True),
        th.Property(
            "token_uri",
            th.StringType,
            default="https://oauth2.googleapis.com/token",
        ),
    ).to_dict()

    def _init_firestore(self):
        """Initialize Firebase app once and return a Firestore client."""
        if not firebase_admin._apps:
            cred = credentials.Certificate(
                {
                    "type": "service_account",
                    "project_id": self.config["project_id"],
                    "private_key_id": self.config["private_key_id"],
                    "private_key": self.config["private_key"],
                    "client_email": self.config["client_email"],
                    "token_uri": self.config.get(
                        "token_uri",
                        "https://oauth2.googleapis.com/token",
                    ),
                }
            )
            firebase_admin.initialize_app(cred)
        return fb_firestore.client()

    def discover_streams(self) -> List[Stream]:
        """Validate tenant status and return one stream per enabled entity."""
        db = self._init_firestore()
        tenant_id = self.config["tenant_id"]

        tenant_doc = db.collection("tenants").document(tenant_id).get()
        if not tenant_doc.exists:
            raise Exception(f"Tenant '{tenant_id}' not found in Firestore.")

        tenant = tenant_doc.to_dict()
        status = tenant.get("status")

        if status != "active":
            raise Exception(
                f"Tenant '{tenant_id}' has status '{status}'. "
                "Only active tenants can sync."
            )

        entities = tenant.get("entities", [])
        streams = []
        for entity in entities:
            stream_class = ENTITY_STREAM_MAP.get(entity)
            if stream_class:
                streams.append(stream_class(tap=self, db=db))
            else:
                self.logger.warning(
                    "No stream class registered for entity '%s' — skipping.",
                    entity,
                )

        return streams


if __name__ == "__main__":
    TapFirestore.cli()
