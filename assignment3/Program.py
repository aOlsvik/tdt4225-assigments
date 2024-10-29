from DbConnector import DbConnector
import pprint

class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
        
    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
        
    def insert_documents(self, collection_name, docs):
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        return documents