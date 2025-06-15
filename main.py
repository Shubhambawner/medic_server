from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from bson import ObjectId

# Import database connection functions
from database import connect_to_mongo, close_mongo_connection, get_collection

# Custom Pydantic type for ObjectId to handle MongoDB's _id
class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, info):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.type = "string"
        field_schema.format = "mongo-objectid"


# Pydantic model for the data you want to dump
class DataToDump(BaseModel):
    message: str = Field(..., min_length=1, max_length=255)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: Optional[str] = "fastapi-dump-api"

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True # Required for PyObjectId
        json_encoders = {
            ObjectId: str # Convert ObjectId to string for JSON serialization
        }


app = FastAPI(
    title="FastAPI MongoDB Data Dumper",
    description="A simple FastAPI app to dump data to MongoDB Atlas.",
    version="1.0.0",
)

# Startup and Shutdown events to manage MongoDB connection
# These functions are now synchronous, matching the pymongo client
@app.on_event("startup")
def startup_event():
    connect_to_mongo() # Call synchronous connect function

@app.on_event("shutdown")
def shutdown_event():
    close_mongo_connection() # Call synchronous close function

@app.get("/dump-data/{message}", summary="Dumps data to MongoDB Atlas")
def dump_data_to_mongodb(message: str): # Changed to 'def' (synchronous)
    """
    This endpoint takes a message as a path parameter and dumps it
    along with a timestamp and source into a MongoDB collection.
    """
    try:
        # Get the collection (e.g., "dumped_data")
        collection = get_collection("dumped_data")

        # Create a Pydantic model instance for the data
        data_entry = DataToDump(message=message)

        # Convert the Pydantic model to a dictionary suitable for MongoDB
        data_dict = data_entry.model_dump(by_alias=True)

        # Insert the data into the collection (no 'await' needed for pymongo)
        result = collection.insert_one(data_dict)

        if result.inserted_id:
            return {
                "status": "success",
                "message": "Data dumped successfully!",
                "inserted_id": str(result.inserted_id),
                "data": data_entry.model_dump_json()
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to insert data into MongoDB."
            )

    except ConnectionError as ce:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"MongoDB connection error: {ce}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# Example: A root endpoint for basic health check
@app.get("/")
def read_root():
    return {"message": "FastAPI app is running. Hit /docs for API documentation."}