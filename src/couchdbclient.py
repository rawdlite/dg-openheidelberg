from config import Config
import couchdb
from typing import List, Dict, Any, Optional

class Client:
    """
    A simple CouchDB client to interact with a CouchDB database.
    """

    def __init__(self):
        """
        Initialize the CouchDB client.
        Loads configuration from the Config class.
        """
        config = Config().get('couchdb')
        couchdb_server = config.get('couchdb_server') or "localhost:5984"
        couchdb_username = config.get('couchdb_username') or ''
        couchdb_password = config.get('couchdb_password') or ''
        database_name = config.get('couchdb_db')
        if couchdb_username and couchdb_password:
            server_url = f"https://{couchdb_username}:{couchdb_password}@{couchdb_server}"
        else:
            server_url = f"http://{couchdb_server}"

        self.server = couchdb.Server(server_url)
        self.db = self.server[database_name]

    @staticmethod
    def mango_filter_by_email(email: str) -> dict:
        """
        Return a Mango filter (selector) for documents with a specific email.
        Example usage: db.find(Client.mango_filter_by_email('user@example.com'))
        """
        return {"email": {"$eq": email}}

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
