"""
DuckDB spatial analytics over Arrow/Parquet geospatial data.
Fast SQL queries over large location datasets without a spatial DB.
SDKs: DuckDB, PyArrow, Polars, H3
"""
import os
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import numpy as np
import h3


class SpatialAnalyticsEngine:
    """
    DuckDB-powered spatial analytics engine.
    Queries Parquet-backed location data with H3 spatial indexing.
    No PostGIS required for analytical workloads.
    """

    def __init__(self, data_dir: str = "./spatial_data", db_path: str = ":memory:"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
        self._setup()
        print(f"[DuckDB Spatial] Engine ready | data_dir={data_dir}")

    def _setup(self):
        self.con.execute("INSTALL spatial; LOAD spatial;")
        self.con.execute("INSTALL httpfs; LOAD httpfs;")

    def ingest_csv(self, csv_path: str, table_name: str = "locations") -> int:
        """Load CSV location data into DuckDB with H3 cell column added."""
        self.con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *,
                lat || ',' || lon AS point_str
            FROM read_csv_auto('{csv_path}', header=true)
        """)
        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[DuckDB] Loaded {count:,} rows into {table_name}")
        return count

    def ingest_dataframe(self, df: Union[pl.DataFrame, pa.Table], table_name: str = "locations"):
        """Register a Polars or Arrow DataFrame as a DuckDB view."""
        if isinstance(df, pl.DataFrame):
            arrow_table = df.to_arrow()
        else:
            arrow_table = df
        self.con.register(table_name, arrow_table)
        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[DuckDB] Registered {count:,} rows as '{table_name}'")

    def add_h3_column(
        self, table_name: str, resolution: int = 7,
        lat_col: str = "lat", lon_col: str = "lon"
    ) -> None:
        """Add H3 cell ID column to existing table."""
        # DuckDB UDF for H3
        def geo_to_h3_udf(lat, lon):
            return h3.geo_to_h3(float(lat), float(lon), resolution)

        self.con.create_function("geo_to_h3", geo_to_h3_udf, ["DOUBLE", "DOUBLE"], "VARCHAR")
        self.con.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN IF NOT EXISTS h3_cell VARCHAR
        """)
        self.con.execute(f"""
            UPDATE {table_name}
            SET h3_cell = geo_to_h3({lat_col}, {lon_col})
        """)
        print(f"[DuckDB] H3 cells added at resolution {resolution}")

    def aggregate_by_h3(
        self,
        table_name: str = "locations",
        value_col: str = "value",
        agg: str = "SUM",
        resolution: Optional[int] = None,
    ) -> pl.DataFrame:
        """Aggregate values by H3 cell. Returns Polars DataFrame."""
        h3_col = f"h3_res{resolution}" if resolution else "h3_cell"
        sql = f"""
            SELECT
                {h3_col} AS h3_index,
                {agg}({value_col}) AS aggregated_value,
                COUNT(*) AS point_count,
                AVG({value_col}) AS mean_value,
                MAX({value_col}) AS max_value
            FROM {table_name}
            WHERE {h3_col} IS NOT NULL
            GROUP BY {h3_col}
            ORDER BY aggregated_value DESC
        """
        result = self.con.execute(sql).fetchdf()
        return pl.from_pandas(result)

    def spatial_density(
        self,
        table_name: str = "locations",
        lat_col: str = "lat",
        lon_col: str = "lon",
        grid_size: float = 0.01,
    ) -> pl.DataFrame:
        """Compute point density on a regular lat/lon grid."""
        sql = f"""
            SELECT
                ROUND({lat_col} / {grid_size}) * {grid_size} AS grid_lat,
                ROUND({lon_col} / {grid_size}) * {grid_size} AS grid_lon,
                COUNT(*) AS density,
                AVG(value) AS mean_value
            FROM {table_name}
            GROUP BY grid_lat, grid_lon
            ORDER BY density DESC
        """
        return pl.from_pandas(self.con.execute(sql).fetchdf())

    def hotspot_analysis(
        self,
        table_name: str = "locations",
        top_n: int = 20,
    ) -> pl.DataFrame:
        """Find top N hotspots by H3 cell value."""
        sql = f"""
            SELECT
                h3_cell,
                SUM(value) AS total_value,
                COUNT(*) AS point_count,
                AVG(value) AS mean_value,
                MAX(value) AS peak_value
            FROM {table_name}
            WHERE h3_cell IS NOT NULL
            GROUP BY h3_cell
            ORDER BY total_value DESC
            LIMIT {top_n}
        """
        result = pl.from_pandas(self.con.execute(sql).fetchdf())
        # Add center coordinates
        centers = []
        for cell in result["h3_cell"].to_list():
            try:
                c = h3.h3_to_geo(cell)
                centers.append({"h3_cell": cell, "center_lat": c[0], "center_lon": c[1]})
            except Exception:
                centers.append({"h3_cell": cell, "center_lat": 0.0, "center_lon": 0.0})
        centers_df = pl.DataFrame(centers)
        return result.join(centers_df, on="h3_cell", how="left")

    def time_series_by_h3(
        self, table_name: str, time_col: str = "timestamp",
        value_col: str = "value", bucket: str = "1 hour"
    ) -> pl.DataFrame:
        """Time-bucketed aggregation per H3 cell."""
        sql = f"""
            SELECT
                TIME_BUCKET(INTERVAL '{bucket}', CAST({time_col} AS TIMESTAMP)) AS bucket,
                h3_cell,
                SUM({value_col}) AS total,
                COUNT(*) AS count
            FROM {table_name}
            GROUP BY bucket, h3_cell
            ORDER BY bucket, total DESC
        """
        return pl.from_pandas(self.con.execute(sql).fetchdf())

    def export_geojson(self, df: pl.DataFrame, output_path: str) -> str:
        """Export H3-indexed DataFrame as GeoJSON FeatureCollection."""
        from spatial.h3_index import H3SpatialIndex
        idx = H3SpatialIndex()
        cells = df["h3_index"].to_list() if "h3_index" in df.columns else []
        values = dict(zip(df["h3_index"].to_list(), df["aggregated_value"].to_list())) if "aggregated_value" in df.columns else {}
        geojson = idx.cells_to_geojson(cells, values)
        import json
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(geojson, f)
        print(f"[DuckDB] GeoJSON exported: {output_path}")
        return output_path

    def close(self):
        self.con.close()
