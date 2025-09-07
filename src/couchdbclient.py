from config import Config
import couchdb
from typing import List, Dict, Any, Optional

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
        if couchdb_username and couchdb_password:
            server_url = f"https://{couchdb_username}:{couchdb_password}@{couchdb_server}"
        else:
            server_url = f"http://{couchdb_server}"
        print(f"**couch**: {self.config}")
        self.server = couchdb.Server(server_url)
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
        mango_query = {"selector": 
            {"member_id": 
                {"$exists": False}
                }
            }
        result = self.db.find(mango_query)
        return list(result)
    
    def get_doc_by_member_id(self, member_id: str):
        """Find doc by member_id"""
        try:
            mid = int(member_id)
        except ValueError:
            print(f"Invalid member_id: {member_id}. It should be an integer.")
            return
        mango_query = {
            "selector": {
                "member_id": {
                "$eq": mid
                }
            }
        }
        result = self.db.find(mango_query)
        return list(result)
    
    def get_doc_by_email(self, email: str):
        """Find doc by email"""
        mango_query = {
            "selector": {
                "email": {
                    "$eq": email
                }
            }
        }
        result = self.db.find(mango_query)
        return list(result) if result else None

    def get_doc_by_nextcloud_id(self, nextcloud_id):
        """Find doc by nextcloud_id"""
        mango_query = {
            "selector": {
                "nextcloud": {
                   "nextcloud_id": {
                       "$eq": nextcloud_id
                   }
               }
            }
        }
        result = self.db.find(mango_query)
        return list(result) if result else None
    
    def get_doc_by_openproject_id(self, openproject_id):
        """Find doc by openproject_id"""
        mango_query = {
            "selector": {
                "openproject": {
                   "openproject_id": {
                       "$eq": openproject_id
                   }
               }
            }
        }
        result = self.db.find(mango_query)
        return list(result) if result else None

    def get_docs_without_openproject_key(self) -> List[Dict[str, Any]]:
        """
        Fetch all documents from CouchDB where the key 'openproject' is not present in the document.
        Returns:
            List of documents without the 'openproject' key
        """
        
        # Mango query: find docs where 'openproject' does not exist
        mango_query = {"selector": 
            {"openproject": 
                {"$exists": False}
                }
            }
        result = self.db.find(mango_query)
        return list(result)

    def get_all_docs(self) -> List[Dict[str, Any]]:
        """
        Fetch all documents from the CouchDB database excluding design documents.
        Returns:
            List of all documents in the database
        """
        rows = self.db.view('app/all_entries')
        return [row.value for row in rows]
