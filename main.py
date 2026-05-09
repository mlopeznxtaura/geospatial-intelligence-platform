"""
geospatial-intelligence-platform — Entry Point

Planetary-scale spatial analysis: H3 indexing, PostGIS queries,
DuckDB analytics, Kafka streaming, and REST API.

Usage:
  python main.py --mode index --data ./data/locations.csv --resolution 7
  python main.py --mode query --lat 37.7749 --lon -122.4194 --radius-km 10
  python main.py --mode analyze --data ./data/locations.csv
  python main.py --mode stream --broker localhost:9092
  python main.py --mode serve --port 8000
  python main.py --mode demo
"""
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Geospatial Intelligence Platform")
    parser.add_argument("--mode", required=True,
                        choices=["index", "query", "analyze", "stream", "serve", "demo"])
    parser.add_argument("--data", default="./data/locations.csv")
    parser.add_argument("--resolution", type=int, default=7)
    parser.add_argument("--lat", type=float, default=37.7749)
    parser.add_argument("--lon", type=float, default=-122.4194)
    parser.add_argument("--radius-km", type=float, default=5.0)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--output", default="./output")
    return parser.parse_args()


def mode_index(args):
    import pandas as pd
    from pathlib import Path
    from spatial.h3_index import H3SpatialIndex, generate_sample_data

    idx = H3SpatialIndex(resolution=args.resolution)
    data_path = Path(args.data)

    if data_path.exists():
        df = pd.read_csv(data_path)
        gdf = idx.dataframe_to_cells(df)
    else:
        print(f"[Main] {args.data} not found — generating synthetic data")
        points = generate_sample_data(n=50_000)
        gdf = idx.points_to_cells(points)

    print(f"
H3 aggregation complete:")
    print(f"  Resolution: {args.resolution}")
    print(f"  H3 cells: {len(gdf):,}")
    print(f"  Total value: {gdf['value'].sum():,.1f}")
    print(f"  Top cells:
{gdf.nlargest(5, 'value')[['h3_index','value','count']]}")

    import json
    from pathlib import Path
    Path(args.output).mkdir(parents=True, exist_ok=True)
    geojson = idx.cells_to_geojson(
        gdf["h3_index"].tolist(),
        dict(zip(gdf["h3_index"].tolist(), gdf["value"].tolist()))
    )
    out = f"{args.output}/h3_aggregation.geojson"
    with open(out, "w") as f:
        json.dump(geojson, f)
    print(f"
GeoJSON saved: {out}")


def mode_query(args):
    from streaming.redis_spatial import RedisSpatialIndex
    from spatial.h3_index import H3SpatialIndex, generate_sample_data

    # Seed Redis with sample data
    geo = RedisSpatialIndex()
    if geo.r and geo.count("demo_locations") == 0:
        points = generate_sample_data(n=1000)
        locs = [{"id": f"poi_{i:04d}", "lat": p[0], "lon": p[1]} for i, p in enumerate(points)]
        geo.add_locations_batch("demo_locations", locs)
        print(f"[Main] Seeded {len(locs)} points into Redis")

    results = geo.nearby("demo_locations", args.lat, args.lon, args.radius_km, count=20)
    print(f"
Nearby results within {args.radius_km}km of ({args.lat}, {args.lon}):")
    for r in results[:10]:
        print(f"  {r['id']}: {r.get('distance_km', '?'):.2f}km away")

    # H3 cell info
    import h3
    cell = h3.geo_to_h3(args.lat, args.lon, 7)
    print(f"
H3 cell (res 7): {cell}")
    print(f"Neighbors: {list(h3.k_ring(cell, 1))[:3]}...")


def mode_analyze(args):
    from analytics.duckdb_spatial import SpatialAnalyticsEngine
    from spatial.h3_index import generate_sample_data
    import pandas as pd
    from pathlib import Path

    engine = SpatialAnalyticsEngine(data_dir=f"{args.output}/spatial_data")

    # Generate or load data
    points = generate_sample_data(n=100_000)
    df = pd.DataFrame(points, columns=["lat", "lon", "value"])
    import polars as pl
    engine.ingest_dataframe(pl.from_pandas(df))
    engine.add_h3_column("locations", resolution=args.resolution)

    hotspots = engine.hotspot_analysis(top_n=10)
    print("
Top 10 Hotspots:")
    print(hotspots.select(["h3_cell", "total_value", "point_count", "center_lat", "center_lon"]))

    density = engine.spatial_density()
    print(f"
Density grid: {len(density)} cells")
    engine.close()


def mode_stream(args):
    import time, random, uuid
    from streaming.kafka_location import LocationEventProducer, LocationEvent

    producer = LocationEventProducer(bootstrap_servers=args.broker)
    rng = random.Random(42)
    print(f"[Main] Streaming location events to {args.broker}... (Ctrl+C to stop)")
    count = 0
    try:
        while True:
            event = LocationEvent(
                event_id=str(uuid.uuid4())[:8],
                user_id=f"user_{rng.randint(0, 1000):04d}",
                lat=37.7749 + rng.gauss(0, 0.05),
                lon=-122.4194 + rng.gauss(0, 0.05),
                timestamp=time.time(),
                event_type=rng.choice(["movement", "checkin", "event"]),
            )
            producer.emit(event)
            count += 1
            if count % 100 == 0:
                print(f"  Published {count} events")
            time.sleep(0.05)
    except KeyboardInterrupt:
        producer.close()
        print(f"
Streamed {count} events")


def mode_serve(args):
    import uvicorn
    from api.server import app
    from prometheus_client import start_http_server
    start_http_server(9090)
    print(f"[Server] Starting on {args.host}:{args.port} | Metrics :9090")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


def mode_demo(args):
    print("Running geospatial demo...
")
    args.output = "./demo_output"
    mode_index(args)
    mode_analyze(args)
    print("
Demo complete. Start 'serve' mode for the live API.")


def main():
    args = parse_args()
    print("=" * 60)
    print("  Geospatial Intelligence Platform")
    print(f"  Mode: {args.mode.upper()}")
    print("=" * 60)

    dispatch = {
        "index": mode_index,
        "query": mode_query,
        "analyze": mode_analyze,
        "stream": mode_stream,
        "serve": mode_serve,
        "demo": mode_demo,
    }
    dispatch[args.mode](args)


if __name__ == "__main__":
    main()
