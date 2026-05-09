"""
Kafka real-time location event ingestion.
Consume location events, enrich with H3 cells, stream to analytics.
SDKs: kafka-python, H3
"""
import json
import time
import threading
from typing import Optional, Callable, List, Dict, Any
from dataclasses import dataclass

import h3
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Histogram

EVENTS_PRODUCED = Counter("geo_events_produced_total", "Location events produced", ["topic"])
EVENTS_CONSUMED = Counter("geo_events_consumed_total", "Location events consumed", ["topic"])
PROCESSING_LATENCY = Histogram("geo_event_processing_ms", "Event processing latency")

LOCATION_TOPIC = "location-events"
H3_TOPIC = "location-h3-enriched"


@dataclass
class LocationEvent:
    event_id: str
    user_id: str
    lat: float
    lon: float
    timestamp: float
    event_type: str = "movement"
    metadata: Dict[str, Any] = None

    def enrich_h3(self, resolution: int = 7) -> Dict[str, Any]:
        d = {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "lat": self.lat,
            "lon": self.lon,
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "h3_res7": h3.geo_to_h3(self.lat, self.lon, 7),
            "h3_res8": h3.geo_to_h3(self.lat, self.lon, 8),
            "h3_res9": h3.geo_to_h3(self.lat, self.lon, 9),
        }
        if self.metadata:
            d.update(self.metadata)
        return d


class LocationEventProducer:
    """Produce location events to Kafka."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self._fallback: List[Dict] = []
        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode(),
                acks=1, linger_ms=10,
            )
            print(f"[Kafka] Location producer connected: {bootstrap_servers}")
        except NoBrokersAvailable:
            print(f"[Kafka] No brokers. Using in-memory fallback.")

    def emit(self, event: LocationEvent, enrich: bool = True) -> bool:
        payload = event.enrich_h3() if enrich else event.__dict__
        topic = H3_TOPIC if enrich else LOCATION_TOPIC
        if self.producer:
            self.producer.send(topic, value=payload)
            EVENTS_PRODUCED.labels(topic=topic).inc()
            return True
        self._fallback.append(payload)
        return False

    def emit_batch(self, events: List[LocationEvent]) -> int:
        count = 0
        for event in events:
            if self.emit(event):
                count += 1
        if self.producer:
            self.producer.flush()
        return count

    def close(self):
        if self.producer:
            self.producer.close()


class LocationEventConsumer:
    """Consume and process location events from Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "geo-analytics",
        topics: Optional[List[str]] = None,
    ):
        self.topics = topics or [H3_TOPIC, LOCATION_TOPIC]
        self.consumer = None
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode()),
                consumer_timeout_ms=1000,
            )
            print(f"[Kafka] Consumer subscribed: {self.topics}")
        except NoBrokersAvailable:
            print("[Kafka] No brokers. Consumer in stub mode.")

    def consume(self, handler: Callable[[Dict], None], max_messages: int = 0):
        if not self.consumer:
            return
        count = 0
        for msg in self.consumer:
            t0 = time.perf_counter()
            handler(msg.value)
            PROCESSING_LATENCY.observe((time.perf_counter() - t0) * 1000)
            EVENTS_CONSUMED.labels(topic=msg.topic).inc()
            count += 1
            if max_messages and count >= max_messages:
                break

    def close(self):
        if self.consumer:
            self.consumer.close()
