"""Custom client handling, including FirestoreStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union

from singer.schema import Schema
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams import Stream


class FirestoreStream(Stream):
    """Stream class for Firestore streams."""

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
        collection: Any = None
    ) -> None:
        super().__init__(name=name, schema=schema, tap=tap)
        self.collection = collection
