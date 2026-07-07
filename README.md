# tap-firestore

`tap-firestore` is a Singer tap and reusable Singer SDK extension for reading receiver change events from Google Firestore.

The main use case is mirroring selected streams from another tap through Firestore. It can also be run directly when stream schemas are provided in config.

## What it reads

The tap reads documents from a Firestore collection, `changes` by default. Each document is expected to include at least:

```json
{
  "tenant_uuid": "tenant-uuid",
  "entity_type": "orders",
  "received_at": "2026-01-01T00:00:00Z",
  "action": "updated",
  "payload": {
    "data": {
      "idorder": 123
    }
  }
}
```

Records are filtered by:

- `tenant_uuid`
- `entity_type`
- `received_at`, using Singer state or `start_date` on the first run

Records are emitted in ascending `received_at` order. The replication key is `received_at`.

Before syncing receiver streams, the tap validates that `tenants/{tenant_uuid}` exists in Firestore and has `status == "active"`.

## Installation

From this repository:

```bash
pipx install git+https://github.com/mariocostaoptiply/tap-firestore.git
```

For local development:

```bash
pipx install poetry
poetry install
poetry run tap-firestore --help
```

## Configuration

### Required Firestore config

| Setting | Required | Description |
| --- | --- | --- |
| `tenant_uuid` | Yes | Firestore tenant document ID to sync. |
| `project_id` | Yes* | Firebase service account project ID. |
| `private_key_id` | Yes* | Firebase service account private key ID. |
| `private_key` | Yes* | Firebase service account private key. Literal `\n` sequences are converted to newlines when read from env/config aliases. |
| `client_email` | Yes* | Firebase service account client email. |
| `token_uri` | No | OAuth token URI. Defaults to `https://oauth2.googleapis.com/token`. |

`project_id`, `private_key_id`, `private_key`, and `client_email` can also be supplied with Firestore-prefixed names:

- `firestore_project_id`
- `firestore_private_key_id`
- `firestore_private_key`
- `firestore_client_email`

The extension also checks environment variables using those same names, including uppercase variants such as `FIRESTORE_PROJECT_ID`.

### Sync config

| Setting | Required | Default | Description |
| --- | --- | --- | --- |
| `collection_name` | No | `changes` | Firestore collection containing change documents. |
| `start_date` | No | unset | Initial lower bound for `received_at` when no state bookmark exists. |
| `tap_streams` | No* | `{}` | Host tap streams to mirror through Firestore. At least one of `tap_streams` or `receiver_only` is required. |
| `receiver_only` | No* | `{}` | Firestore-only streams not paired with a host tap stream. |

### `tap_streams`

`tap_streams` maps host stream names to Firestore receiver stream config. Compact form:

```json
{
  "tap_streams": {
    "orders": "orders",
    "products": "products"
  }
}
```

This creates receiver streams named `receiver_orders` and `receiver_products`, using `orders` and `products` as their Firestore `entity_type` values.

Expanded form:

```json
{
  "tap_streams": {
    "orders": {
      "entity_type": "orders",
      "schema_mode": "inherit",
      "primary_keys": ["idorder"]
    }
  }
}
```

Supported `tap_streams` fields:

| Field | Description |
| --- | --- |
| `entity_type` | Firestore `entity_type` value. Defaults to the stream name. |
| `schema_mode` | `inherit` or `file`. Defaults to `inherit`. |
| `schema_file` | JSON schema path when `schema_mode` is `file`. |
| `primary_keys` | Primary keys for the receiver stream. Defaults to the host stream primary keys. |

Use top-level `collection_name` to select the Firestore collection for all receiver streams in the config.

When `schema_mode` is `inherit`, the receiver schema is copied from the host stream and enriched with:

- `entity_type`
- `received_at`
- `action`
- `extra_fields`

Extra fields from Firestore `payload.data` that are not in the schema are serialized into `extra_fields`.

### `receiver_only`

`receiver_only` creates Firestore streams that are not paired with a host stream. Compact form:

```json
{
  "receiver_only": {
    "deleted_orders": "orders"
  }
}
```

Expanded form:

```json
{
  "receiver_only": {
    "deleted_orders": {
      "entity_type": "orders",
      "schema_mode": "minimal",
      "primary_keys": [],
      "extra_properties": {
        "source": {"type": ["string", "null"]}
      }
    }
  }
}
```

Supported `receiver_only` fields:

| Field | Description |
| --- | --- |
| `entity_type` | Firestore `entity_type` value. Defaults to the stream name. |
| `schema_mode` | `minimal` or `file`. Defaults to `minimal`. |
| `schema_file` | JSON schema path when `schema_mode` is `file`. |
| `primary_keys` | Primary keys for the receiver-only stream. |
| `extra_properties` | Extra JSON Schema properties to add to the minimal schema. |

`receiver_only` streams emit the raw Firestore payload shape:

```json
{
  "entity_type": "orders",
  "received_at": "2026-01-01T00:00:00Z",
  "payload": {
    "data": {}
  }
}
```

## Example direct tap config

```json
{
  "tenant_uuid": "tenant-1",
  "firestore_project_id": "my-firebase-project",
  "firestore_private_key_id": "key-id",
  "firestore_private_key": "<service-account-private-key>",
  "firestore_client_email": "firebase-adminsdk@example.iam.gserviceaccount.com",
  "collection_name": "changes",
  "start_date": "2026-01-01T00:00:00Z",
  "tap_streams": {
    "orders": {
      "entity_type": "orders",
      "schema_mode": "file",
      "schema_file": "schemas/orders.json",
      "primary_keys": ["idorder"]
    }
  },
  "receiver_only": {
    "deleted_orders": {
      "entity_type": "orders",
      "schema_mode": "minimal"
    }
  }
}
```

Run directly:

```bash
tap-firestore --config config.json --discover > catalog.json
tap-firestore --config config.json --catalog catalog.json > output.singer
```

When running `tap-firestore` directly, `tap_streams` entries must use `schema_mode: "file"` with `schema_file`, because there is no host tap stream to inherit a schema from. `schema_mode: "inherit"` is for extension usage inside another tap.

## Using as an extension in another tap

This package exposes `FirestoreExtension` for host taps that want to replace selected full-sync streams with Firestore receiver streams after an initial sync.

Import it in the host tap:

```python
from tap_firestore.extension import FirestoreExtension
```

Typical host-tap integration:

```python
class TapSomething(Tap):
    name = "tap-something"

    def load_state(self, state):
        super().load_state(state)
        if "force_full_sync" in state:
            self.state["force_full_sync"] = list(state.get("force_full_sync", []))

    def discover_streams(self):
        main_streams = [OrdersStream(self), ProductsStream(self)]
        ext_config = self.config.get("firestore_extension")
        if not ext_config or not ext_config.get("enabled", False):
            return main_streams

        extension = FirestoreExtension(tap=self, config=ext_config).initialize()
        return [
            *extension.filter_main_streams(main_streams),
            *extension.discover_streams(main_streams),
        ]

    def sync_all(self):
        ext_config = self.config.get("firestore_extension")
        extension = None
        full_sync_streams = []
        if ext_config and ext_config.get("enabled", False):
            extension = FirestoreExtension(tap=self, config=ext_config)
            full_sync_streams = extension.apply_runtime_selection(self.streams)

        super().sync_all()

        if extension and full_sync_streams:
            extension.write_post_full_sync_bookmarks(full_sync_streams)
```

Host taps usually nest this package's config under a `firestore_extension` key, then pass that nested object to `FirestoreExtension`. The `enabled` flag is a host-tap convention; `FirestoreExtension` itself only receives the nested config once the host has decided to enable it.

Host streams referenced in `tap_streams` must expose `name`, `schema`, and `primary_keys`.

Example host config:

```json
{
  "api_key": "host-api-key",
  "firestore_extension": {
    "enabled": true,
    "tenant_uuid": "tenant-1",
    "firestore_project_id": "my-firebase-project",
    "firestore_private_key_id": "key-id",
    "firestore_private_key": "<service-account-private-key>",
    "firestore_client_email": "firebase-adminsdk@example.iam.gserviceaccount.com",
    "collection_name": "changes",
    "tap_streams": {
      "orders": "orders",
      "products": "products"
    },
    "receiver_only": {
      "deleted_orders": {
        "entity_type": "orders",
        "schema_mode": "minimal"
      }
    }
  }
}
```

At runtime, the extension uses receiver state to decide whether to run the host stream or its Firestore receiver variant:

- If no receiver bookmark exists, the host stream is selected for a full sync.
- If a receiver bookmark exists, the receiver stream is selected and the host stream is disabled.
- A stream listed in top-level state `force_full_sync` will use the host stream even if receiver state exists.

After a full sync, the host tap should call `write_post_full_sync_bookmarks(full_sync_streams)` so future runs can switch to Firestore receiver streams.

Example state to force specific streams back to the host API for one run:

```json
{
  "force_full_sync": ["orders", "products"]
}
```

### Example Picqer mapping

For a Picqer host tap, webhook receiver streams commonly map like this:

| Webhook event(s) | Suggested stream | Notes |
| --- | --- | --- |
| `products.changed` | `receiver_products` / `products` entity | Mirror of `ProductsStream`. |
| `products.free_stock_changed` | `stock` or `deleted_orders` style `receiver_only` stream | Minimal schema/payload stream when it does not match a host stream exactly. |
| `orders.status_changed` | `receiver_orders` / `orders` entity | Mirror of `OrdersStream`. |
| `purchase_orders.changed`, `purchase_orders.purchased`, `purchase_orders.created` | `receiver_purchaseorders` / `purchaseorders` entity | Mirror of `PurchaseOrdersStream`. |
| `receipts.completed`, `receipts.product_received`, `receipts.product_reverted` | `receiver_receipts` / `receipts` entity | Mirror of `ReceiptsStream`. |

Example config fragment:

```json
{
  "collection_name": "picqer_changes",
  "tap_streams": {
    "products": {"entity_type": "receiver_products", "schema_mode": "inherit"},
    "orders": {"entity_type": "receiver_orders", "schema_mode": "inherit"},
    "purchaseorders": {"entity_type": "receiver_purchaseorders", "schema_mode": "inherit"},
    "receipts": {"entity_type": "receiver_receipts", "schema_mode": "inherit"}
  },
  "receiver_only": {
    "stock": {
      "entity_type": "receiver_stock",
      "schema_mode": "minimal"
    }
  }
}
```

## Development

```bash
poetry install
poetry run pytest
poetry run tap-firestore --help
```

The package entrypoint is:

```toml
[tool.poetry.scripts]
tap-firestore = 'tap_firestore.tap:TapFirestore.cli'
```
