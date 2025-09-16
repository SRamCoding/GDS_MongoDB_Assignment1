from fastapi import FastAPI, Query
from pymongo import MongoClient
from bson import ObjectId
from typing import Optional, List
import os
from dotenv import load_dotenv

# ---------- Cargar variables de entorno ----------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

app = FastAPI(title="Transport API", version="1.0")


# ---------- Helpers ----------
def serialize_doc(doc):
    """Convierte ObjectId a str para que FastAPI pueda devolver JSON válido"""
    doc["_id"] = str(doc["_id"])
    return doc


# ---------- 1. Listar viajes con filtros ----------
@app.get("/trips")
def list_trips(
    origin: Optional[str] = Query(None),
    destination: Optional[str] = Query(None),
    vehicle_no: Optional[str] = Query(None),
    ontime: Optional[bool] = Query(None),
):
    query = {}
    if origin:
        query["Origin_Location"] = origin
    if destination:
        query["Destination_Location"] = destination
    if vehicle_no:
        query["vehicle_no"] = vehicle_no
    if ontime is not None:
        query["ontime"] = ontime

    results = list(collection.find(query).limit(50))  # limitar para no sobrecargar
    return [serialize_doc(doc) for doc in results]


# ---------- 2. Obtener viaje por BookingID ----------
@app.get("/trips/{booking_id}")
def get_trip(booking_id: str):
    trip = collection.find_one({"BookingID": booking_id})
    if trip:
        return serialize_doc(trip)
    return {"error": "Trip not found"}


# ---------- 3. Resumen de puntualidad ----------
@app.get("/trips/summary/punctuality")
def punctuality_summary():
    pipeline = [
        {"$group": {"_id": "$ontime", "count": {"$sum": 1}}},
    ]
    results = list(collection.aggregate(pipeline))
    return [{"ontime": r["_id"], "count": r["count"]} for r in results]


# ---------- 4. Top destinos ----------
@app.get("/trips/summary/top-destinations")
def top_destinations(limit: int = 5):
    pipeline = [
        {"$group": {"_id": "$Destination_Location", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit},
    ]
    results = list(collection.aggregate(pipeline))
    return [{"destination": r["_id"], "count": r["count"]} for r in results]


# ---------- 5. Kilómetros promedio por vehículo ----------
@app.get("/trips/summary/avg-km-vehicle")
def avg_km_vehicle():
    pipeline = [
        {
            "$group": {
                "_id": "$vehicle_no",
                "avg_km": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    return [{"vehicle_no": r["_id"], "avg_km": r["avg_km"]} for r in results]


# ---------- 6. Retrasos promedio por cliente ----------
@app.get("/trips/summary/avg-delay-customer")
def avg_delay_customer():
    pipeline = [
        {
            "$group": {
                "_id": "$customerID",
                "avg_delay": {"$avg": "$delay_minutes"},
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    return [{"customerID": r["_id"], "avg_delay": r["avg_delay"]} for r in results]


# ---------- 7. Viajes activos ----------
@app.get("/trips/active")
def active_trips():
    query = {"trip_start_date": {"$ne": None}, "trip_end_date": None}
    results = list(collection.find(query))
    return [serialize_doc(doc) for doc in results]