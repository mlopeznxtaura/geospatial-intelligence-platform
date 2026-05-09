"""
PostGIS client for complex geometric operations and spatial queries.
SDKs: psycopg2, PostGIS, GeoPandas, Shapely
"""
import os
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import geopandas as gpd
import pandas as pd
from shapely import wkb, wkt
from shapely.geometry import Point, Polygon, MultiPolygon


class PostGISClient:
    """
    PostGIS spatial database client.
    Supports point/polygon queries, proximity search, and spatial joins.
    """

    def __init__(self, conn_str: Optional[str] = None):
        self.conn_str = conn_str or os.environ.get(
            "POSTGIS_URL",
            "postgresql://postgres:password@localhost:5432/geospatial"
        )
        self.conn = psycopg2.connect(self.conn_str)
        self._setup_extensions()
        print("[PostGIS] Connected")

    def _setup_extensions(self):
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology")
            self.conn.commit()

    @contextmanager
    def cursor(self):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur

    def create_locations_table(self, table: str = "locations"):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id          SERIAL PRIMARY KEY,
                    location_id TEXT UNIQUE,
                    name        TEXT,
                    category    TEXT,
                    geom        GEOMETRY(Point, 4326),
                    value       DOUBLE PRECISION,
                    timestamp   TIMESTAMPTZ DEFAULT NOW(),
                    metadata    JSONB
                )
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS {table}_geom_idx ON {table} USING GIST(geom)")
            self.conn.commit()

    def insert_point(
        self, location_id: str, lat: float, lon: float,
        name: str = "", category: str = "", value: float = 0,
        metadata: Optional[Dict] = None, table: str = "locations"
    ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table} (location_id, name, category, geom, value, metadata)
                VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)
                ON CONFLICT (location_id) DO UPDATE SET
                    geom = EXCLUDED.geom, value = EXCLUDED.value,
                    timestamp = NOW(), metadata = EXCLUDED.metadata
            """, (location_id, name, category,
                  lon, lat,   # PostGIS uses (lon, lat)
                  value, psycopg2.extras.Json(metadata or {})))
        self.conn.commit()

    def insert_batch(self, points: List[Dict], table: str = "locations"):
        rows = [
            (p["location_id"], p.get("name", ""), p.get("category", ""),
             p["lon"], p["lat"], p.get("value", 0),
             psycopg2.extras.Json(p.get("metadata", {})))
            for p in points
        ]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {table} (location_id, name, category, geom, value, metadata)
                    VALUES %s
                    ON CONFLICT (location_id) DO NOTHING""",
                [(lid, nm, cat, f"ST_SetSRID(ST_MakePoint({lon},{lat}),4326)", v, m)
                 for lid, nm, cat, lon, lat, v, m in rows],
                template="(%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)",
            )
        self.conn.commit()
        print(f"[PostGIS] Inserted {len(rows)} points into {table}")

    def nearby(
        self,
        lat: float, lon: float,
        radius_km: float,
        table: str = "locations",
        limit: int = 100,
        category: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """Find all points within radius_km of (lat, lon). Returns GeoDataFrame."""
        cat_filter = "AND category = %(category)s" if category else ""
        with self.cursor() as cur:
            cur.execute(f"""
                SELECT
                    location_id, name, category, value,
                    ST_Y(geom) AS lat, ST_X(geom) AS lon,
                    ST_Distance(geom::geography,
                        ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography
                    ) / 1000.0 AS distance_km,
                    geom
                FROM {table}
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography,
                    %(radius_m)s
                )
                {cat_filter}
                ORDER BY distance_km
                LIMIT %(limit)s
            """, {
                "lat": lat, "lon": lon,
                "radius_m": radius_km * 1000,
                "category": category,
                "limit": limit,
            })
            rows = cur.fetchall()

        if not rows:
            return gpd.GeoDataFrame()

        df = pd.DataFrame(rows)
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326"
        )
        return gdf

    def cluster_dbscan(
        self, table: str = "locations", eps_km: float = 0.5, min_points: int = 5
    ) -> pd.DataFrame:
        """DBSCAN spatial clustering via PostGIS ST_ClusterDBSCAN."""
        with self.cursor() as cur:
            cur.execute(f"""
                SELECT
                    location_id, name, value,
                    ST_Y(geom) AS lat, ST_X(geom) AS lon,
                    ST_ClusterDBSCAN(geom, eps := %(eps)s, minpoints := %(min)s)
                        OVER () AS cluster_id
                FROM {table}
            """, {"eps": eps_km / 111.0, "min": min_points})  # 1 degree ~= 111km
            return pd.DataFrame(cur.fetchall())

    def spatial_join(
        self, points_table: str, polygons_table: str
    ) -> pd.DataFrame:
        """Join points to containing polygons (e.g., points to neighborhoods)."""
        with self.cursor() as cur:
            cur.execute(f"""
                SELECT p.location_id, p.name AS point_name, p.value,
                       r.name AS region_name, r.category AS region_category
                FROM {points_table} p
                JOIN {polygons_table} r ON ST_Contains(r.geom, p.geom)
            """)
            return pd.DataFrame(cur.fetchall())

    def close(self):
        self.conn.close()
