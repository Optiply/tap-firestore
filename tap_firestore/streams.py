"""Stream type classes for tap-firestore."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable,Tuple

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_firestore.client import FirestoreStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class GenericStream(FirestoreStream):
    """Define custom stream."""
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("document", th.CustomType({"type": ["object"]})),
        th.Property("id",th.StringType)
    ).to_dict()
    def get_records(
        self, context: Optional[dict]
    ) -> Iterable[Union[dict, Tuple[dict, dict]]]:
        """Abstract row generator function. Must be overridden by the child class.

        Each row emitted should be a dictionary of property names to their values.
        Returns either a record dict or a tuple: (record_dict, child_context)

        A method which should retrieve data from the source and return records
        incrementally using the python `yield` operator.

        Only custom stream types need to define this method. REST and GraphQL streams
        should instead use the class-specific methods for REST or GraphQL, respectively.

        This method takes an optional `context` argument, which can be safely ignored
        unless the stream is a child stream or requires partitioning.
        More info: :doc:`/partitioning`.

        Parent streams can optionally return a tuple, in which
        case the second item in the tuple being a `child_context` dictionary for the
        stream's `context`.
        More info: :doc:`/parent_streams`

        Args:
            context: Stream partition or context dictionary.
        """
        docs = self.collection.stream()
        records = []
        for doc in docs:
            records.append({
            "id": doc.id,
            "document": doc.to_dict()
            })
        return records