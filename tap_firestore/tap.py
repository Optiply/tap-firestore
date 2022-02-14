"""Firestore tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
#Firestore imports
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# TODO: Import your custom stream types here:
from tap_firestore.streams import (
    FirestoreStream,
    GenericStream,
    
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    GenericStream
]


class TapFirestore(Tap):
    """Firestore tap class."""
    name = "tap-firestore"

    # TODO: Update this section with the actual config values you expect:
    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "private_key_id",
            th.StringType,
            required=True,
            description="Private key Id for freibase"
        ),
        th.Property(
            "project_id",
            th.StringType,
            required=True,
            description="Project IDs"
        ),
        th.Property(
            "private_key",
            th.StringType,
            description="Private key for firebase."
        ),
        th.Property(
            "auth_uri",
            th.StringType,
            default="https://accounts.google.com/o/oauth2/auth",
            description="The url for the API service"
        ),
        th.Property(
            "token_uri",
            th.StringType,
            default="https://oauth2.googleapis.com/token",
            description="The url for the API service"
        ),
        th.Property(
            "auth_provider_x509_cert_url",
            th.StringType,
            default="https://www.googleapis.com/oauth2/v1/certs",
            description="The url for the API service"
        ),
        th.Property(
            "client_x509_cert_url",
            th.StringType,
            default="https://www.googleapis.com/robot/v1/metadata/x509/tap-firestore%40hotglue.iam.gserviceaccount.com",
            description="Certificate Url"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        cred = credentials.Certificate(dict(self.config))
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        collections = db.collections()
        
        return [GenericStream(tap=self,collection=collection,name=collection.id) for collection in collections]


if __name__ == '__main__':
    TapFirestore.cli()        