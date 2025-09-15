import os
from fastapi import FastAPI, Query
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.json_util import dumps
from bson.objectid import ObjectId
from fastapi.responses import JSONResponse

# ---------- Configuración ----------
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")

client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]

app = FastAPI(title="Logistics API", version="1.0")

# ---------- Endpoints ----------

@app.get("/")
def root():
    return {"message": "🚚 Logistics API is running!"}

# 1. Obtener todos los documentos
@app.get("/shipments")
def get_shipments(status: str = None, origin: str = None, destination: str = None):
    query = {}
    if status:
        query["status"] = status
    if origin:
        query["origin"] = origin
    if destination:
        query["destination"] = destination

    docs = list(collection.find(query))
    return JSONResponse(content=dumps(docs))

# 2. Insertar un nuevo documento
@app.post("/shipments")
def add_shipment(shipment: dict):
    result = collection.insert_one(shipment)
    return {"inserted_id": str(result.inserted_id)}

# 3. Obtener un documento por ID
@app.get("/shipments/{shipment_id}")
def get_shipment(shipment_id: str):
    doc = collection.find_one({"_id": ObjectId(shipment_id)})
    if not doc:
        return {"error": "Shipment not found"}
    return JSONResponse(content=dumps(doc))

# 4. Agregación: peso promedio por destino
@app.get("/stats/avg-weight")
def avg_weight():
    pipeline = [
        {"$group": {"_id": "$destination", "avg_weight": {"$avg": "$weight"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return JSONResponse(content=dumps(result))