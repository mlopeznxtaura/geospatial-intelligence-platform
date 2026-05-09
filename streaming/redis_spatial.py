"""
Redis geospatial index for sub-millisecond proximity queries.
GEOADD/GEORADIUS commands for real-time location lookups.
SDKs: redis
"""
import os
import json
import time
from typing import Optional, List, Dict, Any, Tuple

import redis


class RedisSpatialIndex:
    """
    Redis geospatial index using the GEOADD/GEORADIUS commands.
    Excellent for real-time "find nearby X" queries at <5ms.
    """

    def __init__(self, url: Optional[str] = None, host: str = "localhost", port: int = 6379):
        redis_url = url or os.environ.get("REDIS_URL")
        if redis_url:
            self.r = redis.from_url(redis_url, decode_responses=True)
        else:
            self.r = redis.Redis(host=host, port=port, decode_responses=True)
        try:
            self.r.ping()
            print("[Redis Geo] Spatial index connected")
        except Exception as e:
            print(f"[Redis Geo] Connection failed: {e}")
            self.r = None

    def add_location(
        self,
        key: str,
        member_id: str,
        lat: float,
        lon: float,
        metadata: Optional[Dict] = None,
    ):
        """Add a location to the geospatial index."""
        if self.r is None:
            return
        self.r.geoadd(key, [lon, lat, member_id])
        if metadata:
            self.r.hset(f"geo:meta:{member_id}", mapping={
                k: str(v) for k, v in metadata.items()
            })
            self.r.expire(f"geo:meta:{member_id}", 3600)

    def add_locations_batch(
        self, key: str, locations: List[Dict]
    ):
        """Batch add locations. Each dict: {id, lat, lon, metadata?}"""
        if self.r is None:
            return
        geo_args = []
        for loc in locations:
            geo_args.extend([loc["lon"], loc["lat"], loc["id"]])
        self.r.geoadd(key, geo_args)

    def nearby(
        self,
        key: str,
        lat: float,
        lon: float,
        radius_km: float,
        count: int = 50,
        sort: str = "ASC",
        with_dist: bool = True,
        with_coord: bool = True,
    ) -> List[Dict]:
        """Find all members within radius_km of (lat, lon)."""
        if self.r is None:
            return []
        results = self.r.georadius(
            key, lon, lat,
            radius_km, "km",
            count=count, sort=sort,
            withcoord=with_coord,
            withdist=with_dist,
        )
        parsed = []
        for r in results:
            if with_dist and with_coord:
                member_id, dist, (m_lon, m_lat) = r
                parsed.append({
                    "id": member_id,
                    "distance_km": float(dist),
                    "lat": float(m_lat),
                    "lon": float(m_lon),
                })
            else:
                parsed.append({"id": r if isinstance(r, str) else r[0]})
        return parsed

    def nearby_member(
        self, key: str, member_id: str, radius_km: float, count: int = 50
    ) -> List[Dict]:
        """Find members near an existing indexed member."""
        if self.r is None:
            return []
        results = self.r.georadiusbymember(
            key, member_id, radius_km, "km",
            count=count, sort="ASC",
            withdist=True, withcoord=True,
        )
        return [
            {"id": r[0], "distance_km": float(r[1]), "lat": float(r[2][1]), "lon": float(r[2][0])}
            for r in results
            if r[0] != member_id
        ]

    def get_position(self, key: str, member_id: str) -> Optional[Tuple[float, float]]:
        """Get (lat, lon) of a member."""
        if self.r is None:
            return None
        pos = self.r.geopos(key, member_id)
        if pos and pos[0]:
            lon, lat = pos[0]
            return float(lat), float(lon)
        return None

    def distance_km(self, key: str, member1: str, member2: str) -> Optional[float]:
        """Get distance between two members in km."""
        if self.r is None:
            return None
        dist = self.r.geodist(key, member1, member2, unit="km")
        return float(dist) if dist else None

    def update_location(self, key: str, member_id: str, lat: float, lon: float):
        """Update position of an existing member."""
        self.add_location(key, member_id, lat, lon)

    def remove(self, key: str, *member_ids: str):
        """Remove members from spatial index."""
        if self.r:
            self.r.zrem(key, *member_ids)

    def count(self, key: str) -> int:
        if self.r is None:
            return 0
        return self.r.zcard(key)
