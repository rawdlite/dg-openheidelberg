from config import Config
import couchdb2
import os
from typing import List, Dict, Any, Optional

DEBUG = os.getenv("DEBUG", "0") in ("1", "true", "True")

class Client:
    """
    A simple CouchDB client to interact with a CouchDB database.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the CouchDB client.
        Loads configuration from the Config class.
        """
        self.config = config or Config().get('couchdb')
        couchdb_server = self.config.get('couchdb_server') or "localhost:5984"
        couchdb_username = self.config.get('couchdb_username') or ''
        couchdb_password = self.config.get('couchdb_password') or ''
        database_name = self.config.get('couchdb_db')
        if not database_name:
            raise ValueError("CouchDB database name must be specified in the configuration.")
        server_url = f"https://{couchdb_server}"
        if DEBUG:
            print(f"**couch config**: {self.config}")
        self.server = couchdb2.Server(
            username=couchdb_username,
            password=couchdb_password,
            href=server_url
        )
        self.db = self.server[database_name]

    @staticmethod
    def mango_filter_by_email(email: str) -> dict:
        """
        Return a Mango filter (selector) for documents with a specific email.
        Example usage: db.find(Client.mango_filter_by_email('user@example.com'))
        """
        return {"email": {"$eq": email}}
    
    def get_docs_without_member_id(self) -> List[Dict[str, Any]]:
        """
        Fetch all documents from CouchDB where the key 'member_id' is not present in the document.
        Returns:
            List of documents without the 'openpromember_idject' key
        """
        # Mango query: find docs where 'member_id' does not exist

        selector = {"member_id": {"$exists": False}}
        result = self.db.find(selector=selector )
        return list(result.get('docs', []))

    def get_doc_by_member_id(self, member_id: str):
        """Find doc by member_id"""
        try:
            mid = int(member_id)
        except ValueError:
            print(f"Invalid member_id: {member_id}. It should be an integer.")
            return
        selector = {
            "member_id": {
            "$eq": mid
             }
        }
        result = self.db.find(selector=selector)
        return list(result.get('docs', [])) if result else None

    def get_doc_by_email(self, email: str):
        """Find doc by email"""
        selector = {
            "email": {
                "$eq": email
                }
            }
        result = self.db.find(selector=selector)
        return list(result.get('docs', [])) if result else None

    def get_doc_by_nextcloud_id(self, nextcloud_id):
        """Find doc by nextcloud_id"""
        selector = {
            "nextcloud": {
                "nextcloud_id": {
                    "$eq": nextcloud_id
                }
            }
        }
        result = self.db.find(selector=selector)
        return list(result.get('docs', [])) if result else None

    def get_doc_by_openproject_id(self, openproject_id):
        """Find doc by openproject_id"""
        selector = {
            "openproject": {
                "openproject_id": {
                    "$eq": openproject_id
                }
            }
        }
        result = self.db.find(selector=selector)
        return list(result.get('docs', [])) if result else None

    def get_docs_without_openproject_key(self) -> List[Dict[str, Any]]:
        """
        Fetch all documents from CouchDB where the key 'openproject' is not present in the document.
        Returns:
            List of documents without the 'openproject' key
        """
        
        # Mango query: find docs where 'openproject' does not exist
        selector = {
            "openproject": {
                "$exists": False
            }
        }
        result = self.db.find(selector=selector)
        return list(result.get('docs', []))

    def get_all_docs(self) -> List[Dict[str, Any]]:
        """
        Fetch all documents from the CouchDB database excluding design documents.
        Returns:
            List of all documents in the database
        """
        rows = self.db.view('apps', 'all_entries', include_docs=True)
        return [row.doc for row in rows if not row.id.startswith('_design/')]
