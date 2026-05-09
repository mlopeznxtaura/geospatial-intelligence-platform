# Geospatial Intelligence Platform

Cluster 19 of the NextAura 500 SDKs / 25 Clusters project.

Real-time planetary-scale spatial analysis and visualization. H3 hexagonal indexing makes spatial aggregation uniform and scalable at any zoom level.

## Architecture

- H3 hexagonal spatial indexing for uniform aggregation at any resolution
- PostGIS for complex geometric operations and spatial queries
- GDAL for reading every geospatial format (GeoTIFF, Shapefile, GeoJSON, etc.)
- GeoPandas + PyProj for spatial data manipulation and coordinate transforms
- DuckDB + Arrow + Polars for the analytical query layer
- Kafka for real-time location event stream ingestion
- Redis for spatial index caching
- Deck.gl / Kepler.gl / Cesium for 3D visualization layers
- HuggingFace for satellite image classification
- FastAPI for the geospatial REST API

## SDKs Used

Deck.gl, Kepler.gl SDK, H3 SDK, PostGIS SDK, GDAL SDK, GeoPandas, Cesium SDK, Mapbox GL JS, MapLibre GL JS, Turf.js, PyProj SDK, OpenStreetMap SDK, DuckDB, Apache Arrow, Polars, Kafka SDK, FastAPI, Redis SDK, Prometheus Client, HuggingFace Transformers

## Quickstart

```bash
pip install -r requirements.txt
docker-compose up -d  # PostGIS, Kafka, Redis

# Index location data with H3
python main.py --mode index --data ./data/locations.csv --resolution 7

# Run spatial queries
python main.py --mode query --lat 37.7749 --lon -122.4194 --radius-km 10

# Stream location events
python main.py --mode stream --topic location-events

# Start API server
python main.py --mode serve
```

## Structure

```
spatial/
  h3_index.py       H3 hexagonal indexing and aggregation (entry point)
  postgis_client.py PostGIS spatial queries
  gdal_reader.py    GDAL multi-format geospatial file reader
  geopandas_ops.py  GeoPandas spatial operations and transforms
analytics/
  duckdb_spatial.py DuckDB spatial analytics over Arrow/Parquet
  heatmap.py        Kernel density estimation and heatmap generation
streaming/
  kafka_location.py Kafka real-time location event ingestion
  redis_spatial.py  Redis geospatial index and proximity queries
api/
  server.py         FastAPI geospatial REST API
main.py             Entry point
```
