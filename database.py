import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# Load environment variables from .env file
load_dotenv()

MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

if not MONGO_CONNECTION_STRING:
    raise ValueError("MONGO_CONNECTION_STRING not found in .env file")
if not MONGO_DB_NAME:
    raise ValueError("MONGO_DB_NAME not found in .env file")

client: AsyncIOMotorClient = None
db = None

async def connect_to_mongo():
    """Connects to MongoDB Atlas."""
    global client, db
    try:
        client = AsyncIOMotorClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DB_NAME]
        print(f"Connected to MongoDB database: {MONGO_DB_NAME}")
    except Exception as e:
        print(f"Could not connect to MongoDB: {e}")
        # You might want to re-raise the exception or handle it differently
        # depending on your application's error handling strategy.

async def close_mongo_connection():
    """Closes the MongoDB connection."""
    global client
    if client:
        client.close()
        print("MongoDB connection closed.")

# You can add a helper function for the collection if you have a specific one
def get_collection(collection_name: str):
    """Returns a specific MongoDB collection."""
    if db:
        return db[collection_name]
    else:
        raise ConnectionError("MongoDB not connected.")
