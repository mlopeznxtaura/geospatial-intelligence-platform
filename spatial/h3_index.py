"""
H3 hexagonal spatial indexing and aggregation.
Entry point per the spec: (lat, lon, value) tuples -> H3 cells -> aggregated GeoDataFrame.
SDKs: H3, GeoPandas, Shapely, PyProj
"""
import json
import numpy as np
from typing import List, Tuple, Dict, Any, Optional, Union
from dataclasses import dataclass

import h3
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, Point, mapping
from pyproj import Transformer, CRS


@dataclass
class H3Cell:
    cell_id: str
    resolution: int
    center_lat: float
    center_lon: float
    value: float
    count: int
    geometry: Polygon


class H3SpatialIndex:
    """
    H3 hexagonal spatial indexing for uniform aggregation at any resolution.

    Resolution guide:
      res 4  = ~86km avg edge  (~city region)
      res 5  = ~33km avg edge  (~district)
      res 6  = ~12km avg edge  (~neighborhood)
      res 7  = ~4.5km avg edge (~block group)
      res 8  = ~1.7km avg edge (~block)
      res 9  = ~0.6km avg edge (~building cluster)
      res 10 = ~0.2km avg edge (~single building)
    """

    def __init__(self, resolution: int = 7):
        self.resolution = resolution

    def points_to_cells(
        self,
        points: List[Tuple[float, float, float]],
        agg_fn: str = "sum",
    ) -> gpd.GeoDataFrame:
        """
        Convert (lat, lon, value) tuples to H3 cells with aggregated values.
        Returns GeoDataFrame with H3 polygon geometries.

        Args:
            points: list of (lat, lon, value) tuples
            agg_fn: aggregation function — 'sum', 'mean', 'count', 'max', 'min'

        Returns:
            GeoDataFrame with columns: h3_index, value, count, geometry
        """
        if not points:
            return gpd.GeoDataFrame(columns=["h3_index", "value", "count", "geometry"])

        # Map each point to its H3 cell
        cell_data: Dict[str, List[float]] = {}
        for lat, lon, val in points:
            cell_id = h3.geo_to_h3(lat, lon, self.resolution)
            if cell_id not in cell_data:
                cell_data[cell_id] = []
            cell_data[cell_id].append(val)

        # Aggregate values per cell
        agg_funcs = {
            "sum": sum,
            "mean": lambda x: sum(x) / len(x),
            "count": len,
            "max": max,
            "min": min,
        }
        agg = agg_funcs.get(agg_fn, sum)

        rows = []
        for cell_id, values in cell_data.items():
            center = h3.h3_to_geo(cell_id)
            boundary = h3.h3_to_geo_boundary(cell_id)
            # h3 returns (lat, lon) pairs; shapely expects (lon, lat)
            polygon = Polygon([(lon, lat) for lat, lon in boundary])
            rows.append({
                "h3_index": cell_id,
                "value": float(agg(values)),
                "count": len(values),
                "center_lat": center[0],
                "center_lon": center[1],
                "geometry": polygon,
            })

        gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        return gdf

    def dataframe_to_cells(
        self,
        df: pd.DataFrame,
        lat_col: str = "lat",
        lon_col: str = "lon",
        value_col: str = "value",
        agg_fn: str = "sum",
    ) -> gpd.GeoDataFrame:
        """Convert a pandas DataFrame to H3-indexed GeoDataFrame."""
        points = list(zip(df[lat_col], df[lon_col], df[value_col]))
        return self.points_to_cells(points, agg_fn=agg_fn)

    def get_neighbors(self, cell_id: str, k: int = 1) -> List[str]:
        """Get all H3 cells within k rings of a given cell."""
        return list(h3.k_ring(cell_id, k))

    def compact(self, cells: List[str]) -> List[str]:
        """Compact a set of H3 cells to the coarsest representation."""
        return list(h3.compact(cells))

    def uncompact(self, cells: List[str], target_resolution: int) -> List[str]:
        """Expand cells to finer resolution."""
        return list(h3.uncompact(cells, target_resolution))

    def polyfill(self, geojson_polygon: dict) -> List[str]:
        """Fill a GeoJSON polygon with H3 cells at current resolution."""
        return list(h3.polyfill(geojson_polygon, self.resolution))

    def polyfill_geodataframe(self, gdf: gpd.GeoDataFrame) -> List[str]:
        """Fill all polygons in a GeoDataFrame with H3 cells."""
        cells = set()
        for geom in gdf.geometry:
            geojson = mapping(geom)
            cells.update(h3.polyfill(geojson, self.resolution))
        return list(cells)

    def cells_to_geojson(self, cells: List[str], values: Optional[Dict[str, float]] = None) -> dict:
        """Convert H3 cells to GeoJSON FeatureCollection for frontend visualization."""
        features = []
        for cell_id in cells:
            boundary = h3.h3_to_geo_boundary(cell_id)
            coords = [[lon, lat] for lat, lon in boundary]
            coords.append(coords[0])  # Close polygon
            feature = {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": {
                    "h3_index": cell_id,
                    "resolution": h3.h3_get_resolution(cell_id),
                    "value": values.get(cell_id, 0) if values else 0,
                },
            }
            features.append(feature)
        return {"type": "FeatureCollection", "features": features}

    def change_resolution(
        self, gdf: gpd.GeoDataFrame, new_resolution: int
    ) -> gpd.GeoDataFrame:
        """Re-aggregate a GeoDataFrame to a different H3 resolution."""
        points = []
        for _, row in gdf.iterrows():
            center = h3.h3_to_geo(row["h3_index"])
            points.append((center[0], center[1], row["value"]))
        idx = H3SpatialIndex(resolution=new_resolution)
        return idx.points_to_cells(points, agg_fn="sum")

    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Haversine distance in kilometers between two points."""
        R = 6371.0
        phi1, phi2 = np.radians(lat1), np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = np.sin(dphi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2)**2
        return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def generate_sample_data(n: int = 10_000, bbox: Tuple = (37.6, -122.6, 37.9, -122.2)) -> List[Tuple]:
    """Generate synthetic (lat, lon, value) data for testing."""
    rng = np.random.default_rng(42)
    lat_min, lon_min, lat_max, lon_max = bbox
    lats = rng.uniform(lat_min, lat_max, n)
    lons = rng.uniform(lon_min, lon_max, n)
    # Hot spots around city center
    center_lat, center_lon = (lat_min + lat_max) / 2, (lon_min + lon_max) / 2
    values = np.exp(-((lats - center_lat)**2 + (lons - center_lon)**2) * 200) * 100
    values += rng.exponential(5, n)
    return list(zip(lats.tolist(), lons.tolist(), values.tolist()))
