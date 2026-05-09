"""
FastAPI geospatial REST API.
Spatial queries, H3 aggregation, and streaming ingest endpoints.
SDKs: FastAPI, H3, DuckDB, Redis, PostGIS
"""
import os
import time
import json
import uuid
from typing import Optional, List, Dict, Any

import numpy as np
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, start_http_server

app = FastAPI(title="Geospatial Intelligence Platform API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

GEO_REQUESTS = Counter("geo_api_requests_total", "API requests", ["endpoint"])
GEO_LATENCY = Histogram("geo_api_latency_ms", "API latency", buckets=[5, 10, 25, 50, 100, 250, 500])

_spatial_index = None
_redis_geo = None
_analytics = None


def get_redis_geo():
    global _redis_geo
    if _redis_geo is None:
        from streaming.redis_spatial import RedisSpatialIndex
        _redis_geo = RedisSpatialIndex(url=os.environ.get("REDIS_URL"))
    return _redis_geo


class LocationPoint(BaseModel):
    lat: float
    lon: float
    value: float = 1.0
    category: Optional[str] = None
    metadata: Optional[Dict] = None


class LocationBatch(BaseModel):
    points: List[LocationPoint]


class H3AggRequest(BaseModel):
    points: List[List[float]]  # [[lat, lon, value], ...]
    resolution: int = 7
    agg_fn: str = "sum"


class NearbyRequest(BaseModel):
    lat: float
    lon: float
    radius_km: float = 5.0
    limit: int = 50
    category: Optional[str] = None


@app.post("/h3/aggregate")
async def h3_aggregate(req: H3AggRequest):
    """Aggregate (lat,lon,value) points into H3 cells."""
    t0 = time.perf_counter()
    GEO_REQUESTS.labels(endpoint="h3_aggregate").inc()

    from spatial.h3_index import H3SpatialIndex
    idx = H3SpatialIndex(resolution=req.resolution)
    points = [(p[0], p[1], p[2] if len(p) > 2 else 1.0) for p in req.points]
    gdf = idx.points_to_cells(points, agg_fn=req.agg_fn)
    geojson = idx.cells_to_geojson(
        gdf["h3_index"].tolist(),
        dict(zip(gdf["h3_index"].tolist(), gdf["value"].tolist()))
    )

    elapsed = (time.perf_counter() - t0) * 1000
    GEO_LATENCY.observe(elapsed)
    geojson["meta"] = {"n_cells": len(gdf), "resolution": req.resolution, "latency_ms": round(elapsed, 2)}
    return geojson


@app.get("/nearby")
async def nearby(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius_km: float = Query(5.0, description="Search radius in km"),
    limit: int = Query(50, description="Max results"),
    key: str = Query("locations", description="Redis geo key"),
):
    """Find locations near a point using Redis geo index."""
    t0 = time.perf_counter()
    GEO_REQUESTS.labels(endpoint="nearby").inc()

    geo = get_redis_geo()
    results = geo.nearby(key, lat, lon, radius_km, count=limit)
    elapsed = (time.perf_counter() - t0) * 1000
    GEO_LATENCY.observe(elapsed)
    return {"results": results, "count": len(results), "latency_ms": round(elapsed, 2)}


@app.post("/locations/batch")
async def ingest_locations(batch: LocationBatch, background_tasks: BackgroundTasks):
    """Ingest a batch of location points into Redis geo index."""
    geo = get_redis_geo()
    locs = [
        {"id": f"loc_{uuid.uuid4().hex[:8]}", "lat": p.lat, "lon": p.lon}
        for p in batch.points
    ]
    background_tasks.add_task(geo.add_locations_batch, "locations", locs)
    GEO_REQUESTS.labels(endpoint="ingest_batch").inc()
    return {"status": "queued", "count": len(locs)}


@app.get("/h3/cell")
async def h3_cell_info(lat: float, lon: float, resolution: int = 7):
    """Get H3 cell info for a lat/lon point."""
    import h3
    cell_id = h3.geo_to_h3(lat, lon, resolution)
    center = h3.h3_to_geo(cell_id)
    neighbors = list(h3.k_ring(cell_id, 1))
    return {
        "h3_index": cell_id,
        "resolution": resolution,
        "center": {"lat": center[0], "lon": center[1]},
        "neighbor_count": len(neighbors) - 1,
        "neighbors": neighbors[:6],
    }


@app.get("/health")
async def health():
    geo = get_redis_geo()
    return {
        "status": "ok",
        "redis": geo.r is not None if geo else False,
        "location_count": geo.count("locations") if geo and geo.r else 0,
    }
