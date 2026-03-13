"""Stream definitions for tap-firestore."""

from singer_sdk import typing as th

from tap_firestore.client import ChangesStream


class ProductsStream(ChangesStream):
    """Products stream — reads product change events from Firestore."""

    name = "products"
    entity_type = "products"
    primary_keys = ["idproduct"]
    replication_key = "received_at"

    schema = th.PropertiesList(
        # Primary key
        th.Property("idproduct", th.IntegerType),
        # Identity
        th.Property("name", th.StringType),
        th.Property("productcode", th.StringType),
        th.Property("productcode_supplier", th.StringType),
        th.Property("barcode", th.StringType),
        th.Property("description", th.StringType),
        th.Property("type", th.StringType),
        # Flags
        th.Property("active", th.BooleanType),
        th.Property("assembled", th.BooleanType),
        th.Property("unlimitedstock", th.BooleanType),
        th.Property("show_on_portal", th.BooleanType),
        # Pricing & dimensions
        th.Property("price", th.NumberType),
        th.Property("fixedstockprice", th.NumberType),
        th.Property("weight", th.NumberType),
        th.Property("height", th.NumberType),
        th.Property("length", th.NumberType),
        th.Property("width", th.NumberType),
        # Purchasing
        th.Property("minimum_purchase_quantity", th.IntegerType),
        th.Property("purchase_in_quantities_of", th.IntegerType),
        th.Property("deliverytime", th.IntegerType),
        # Relations
        th.Property("idsupplier", th.IntegerType),
        th.Property("idvatgroup", th.IntegerType),
        # Analytics
        th.Property("comment_count", th.IntegerType),
        th.Property("analysis_abc_classification", th.StringType),
        th.Property("analysis_pick_amount_per_day", th.NumberType),
        # Trade
        th.Property("hs_code", th.StringType),
        th.Property("country_of_origin", th.StringType),
        # Timestamps (plain strings from Picqer)
        th.Property("created", th.StringType),
        th.Property("updated", th.StringType),
        # Nested collections
        th.Property(
            "stock",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idwarehouse", th.IntegerType),
                    th.Property("stock", th.IntegerType),
                    th.Property("freestock", th.IntegerType),
                    th.Property("reserved", th.IntegerType),
                    th.Property("reservedpicklists", th.IntegerType),
                    th.Property("reservedbackorders", th.IntegerType),
                    th.Property("reservedallocations", th.IntegerType),
                )
            ),
        ),
        th.Property(
            "images",
            th.ArrayType(th.CustomType({"type": ["object", "string"]})),
        ),
        th.Property(
            "pricelists",
            th.ArrayType(th.CustomType({"type": ["object"]})),
        ),
        th.Property(
            "productfields",
            th.ArrayType(th.CustomType({"type": ["object"]})),
        ),
        th.Property("tags", th.CustomType({"type": ["object"]})),
        # Metadata from the changes document
        th.Property("received_at", th.DateTimeType),
        th.Property("action", th.StringType),
    ).to_dict()


class OrdersStream(ChangesStream):
    """Orders stream — full implementation pending."""

    name = "orders"
    entity_type = "orders"
    primary_keys = ["idorder"]
    replication_key = "received_at"

    schema = th.PropertiesList(
        th.Property("idorder", th.IntegerType),
        th.Property("received_at", th.DateTimeType),
        th.Property("action", th.StringType),
    ).to_dict()

    def get_records(self, context):
        self.logger.info("Orders stream is not yet implemented — skipping.")
        return iter([])


class PurchaseOrdersStream(ChangesStream):
    """Purchase orders stream — full implementation pending."""

    name = "purchase_orders"
    entity_type = "purchase_orders"
    primary_keys = ["idpurchaseorder"]
    replication_key = "received_at"

    schema = th.PropertiesList(
        th.Property("idpurchaseorder", th.IntegerType),
        th.Property("received_at", th.DateTimeType),
        th.Property("action", th.StringType),
    ).to_dict()

    def get_records(self, context):
        self.logger.info("Purchase orders stream is not yet implemented — skipping.")
        return iter([])


class ReceiptsStream(ChangesStream):
    """Receipts stream — full implementation pending."""

    name = "receipts"
    entity_type = "receipts"
    primary_keys = ["idreceipt"]
    replication_key = "received_at"

    schema = th.PropertiesList(
        th.Property("idreceipt", th.IntegerType),
        th.Property("received_at", th.DateTimeType),
        th.Property("action", th.StringType),
    ).to_dict()

    def get_records(self, context):
        self.logger.info("Receipts stream is not yet implemented — skipping.")
        return iter([])


class SuppliersStream(ChangesStream):
    """Suppliers stream — full implementation pending."""

    name = "suppliers"
    entity_type = "suppliers"
    primary_keys = ["idsupplier"]
    replication_key = "received_at"

    schema = th.PropertiesList(
        th.Property("idsupplier", th.IntegerType),
        th.Property("received_at", th.DateTimeType),
        th.Property("action", th.StringType),
    ).to_dict()

    def get_records(self, context):
        self.logger.info("Suppliers stream is not yet implemented — skipping.")
        return iter([])
