"""Firebase auth helpers for Firestore extension usage."""

from typing import Any, Dict

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore as fb_firestore


def get_firestore_client(config: Dict[str, Any]):
    """Initialize Firebase once and return a Firestore client."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(
            {
                "type": config.get("type", "service_account"),
                "project_id": config["project_id"],
                "private_key_id": config["private_key_id"],
                "private_key": config["private_key"],
                "client_email": config["client_email"],
                "token_uri": config.get(
                    "token_uri",
                    "https://oauth2.googleapis.com/token",
                ),
            }
        )
        firebase_admin.initialize_app(cred)
    return fb_firestore.client()
