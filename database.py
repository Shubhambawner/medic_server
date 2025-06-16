import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables from .env file
load_dotenv()

MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

if not MONGO_CONNECTION_STRING:
    raise ValueError("MONGO_CONNECTION_STRING not found in .env file")
if not MONGO_DB_NAME:
    raise ValueError("MONGO_DB_NAME not found in .env file")

# Global variables to hold the client and database instance
client: MongoClient = None
db = None

def connect_to_mongo():
    """Connects to MongoDB Atlas using pymongo."""
    global client, db
    try:
        client = MongoClient(MONGO_CONNECTION_STRING, server_api=ServerApi('1'))
        db = client[MONGO_DB_NAME]
        # Ping to confirm a successful connection
        client.admin.command('ping')
        print(f"Pinged your deployment. Successfully connected to MongoDB database: {MONGO_DB_NAME}!")
    except Exception as e:
        print(f"Could not connect to MongoDB: {e}")
        # In a real application, you might want to exit or log a critical error
        raise

def close_mongo_connection():
    """Closes the MongoDB connection."""
    global client
    if client:
        client.close()
        print("MongoDB connection closed.")

def get_collection(collection_name: str = 'medic'):
    """Returns a specific MongoDB collection."""
    if db is not None:
        return db[collection_name]
    else:
        # This error should ideally not be hit if startup event works correctly
        raise ConnectionError("MongoDB not connected. Ensure connect_to_mongo was called.")
    
    
if __name__ == "__main__":
    connect_to_mongo()