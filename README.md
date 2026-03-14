# PT Analytics - Real-Time Demand Analysis for Public Transport

Passenger demand analytics platform for Liverpool bus network using dwell time patterns. Processes real-time GPS data from 400+ buses to identify high-demand stops, peak hours, and route capacity requirements.

**Live API:** [api.heyico.work](https://api.heyico.work/docs)

---

## Overview

PT Analytics transforms raw vehicle position data into actionable transit demand insights. The system processes GPS feeds from the UK Bus Open Data Service and calculates dwell time patterns—the duration buses spend at stops—as a validated proxy for passenger activity.

**Current Coverage:**
- 137 routes across Liverpool City Region
- 1,000+ stops with demand data
- 11 transit operators monitored
- Real-time updates every 10 minutes

**Key Applications:**
- Infrastructure investment prioritisation
- Schedule frequency optimisation
- Route capacity planning
- Peak hour demand analysis

---

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│  UK Bus Open Data Service (BODS) - Government API           │
│  Real-time GPS positions for 400+ buses                     │
└───────────────────────┬─────────────────────────────────────┘
                        │ Poll every 10 seconds
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  VM#1 - Data Processing Pipeline (1GB RAM)                  │
│  ┌───────────────────────────────────────────────────┐      │
│  │ Cron-Scheduled Jobs:                              │      │
│  │  • Ingestion:    Every 10s  → Collect positions   │      │
│  │  • Analysis:     Every 10m  → Detect stop events  │      │
│  │  • Aggregation:  Every 10m  → Calculate averages  │      │
│  │  • Cleanup:      Every 10m  → Maintain 10m window │      │
│  └───────────────────────────────────────────────────┘      │
│  ┌───────────────────────────────────────────────────┐      │
│  │ FastAPI + nginx (api.heyico.work)                 │      │
│  │  • Async endpoints with connection pooling        │      │
│  │  • API key authentication (SHA-256 hashed)        │      │
│  │  • Request logging & rate limiting                │      │
│  │  • HTTPS via Let's Encrypt                        │      │
│  └───────────────────────────────────────────────────┘      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  VM#2 - PostgreSQL 14 + PostGIS (1GB RAM)                   │
│                                                             │
│  Raw Data (10-min retention):                               │
│   • vehicle_positions    ~5MB (rolling window)              │
│   • vehicle_arrivals     temporary (deleted after agg)      │
│                                                             │
│  Static Data:                                               │
│   • txc_stops            6,395 stops with coordinates       │
│   • txc_route_patterns   556 route variants                 │
│   • txc_pattern_stops    21,130 route-stop mappings         │
│                                                             │
│  Analytics (permanent, fixed-size):                         │
│   • dwell_time_analysis  Running averages by:               │
│     stop × route × direction × operator × day × hour        │
│                                                             │
│  Auth:                                                      │
│   • api_keys             Hashed keys with user metadata     │
│                                                             │
│  Total Size: <50MB (stable)                                 │
└─────────────────────────────────────────────────────────────┘
```

**Infrastructure:** Oracle Cloud Always Free tier (2× VMs), self-hosted PostgreSQL, nginx reverse proxy with Let's Encrypt SSL

---

## Technical Implementation

### Data Pipeline

**1. Ingestion (Every 10 seconds)**
- Poll UK government BODS API for Liverpool region
- Parse SIRI-VM XML vehicle positions
- Batch insert to `vehicle_positions` table
- Mark all as `analyzed = false`

**Throughput:** ~450 positions per minute

---

**2. Analysis (Every 10 minutes)**

Stop detection algorithm:
```python
# Identify stop events from GPS patterns
for vehicle in unanalyzed_positions:
    if stationary_for(vehicle, duration=20s):
        stop_event = {
            'dwell_time': count_stationary_seconds(vehicle),
            'location': vehicle.lat_lon,
            'route': vehicle.route_name
        }
```

Spatial matching optimisation:
```python
class StopMatcher:
    def __init__(self):
        # Load 6,395 stops once into memory
        # Index: {route → direction → [stops with coords]}
        self.route_stops = build_stop_index()
    
    def match(self, stop_event):
        # In-memory haversine distance calculation
        # 100x faster than database queries per event
        candidates = self.route_stops[event.route][event.direction]
        return find_nearest(candidates, event.lat, event.lon, radius=30m)
```

**Performance:** Processes 100+ stop events in <5 seconds

---

**3. Aggregation (Every 10 minutes)**

Running average calculation:
```sql
INSERT INTO dwell_time_analysis 
  (stop_id, route, direction, operator, day_of_week, hour_of_day, 
   avg_dwell, stddev_dwell, sample_count)
SELECT ...
FROM vehicle_arrivals
GROUP BY stop_id, route, direction, operator, day_of_week, hour_of_day
ON CONFLICT (stop_id, route, direction, operator, day_of_week, hour_of_day)
DO UPDATE SET
  avg_dwell = (old.avg × old.count + new.sum) / (old.count + new.count),
  sample_count = old.count + new.count,
  stddev_dwell = calculate_stddev(new_values);
```

**Result:** Fixed-size table (~500K rows max) regardless of data volume

---

**4. Cleanup (Every 10 minutes)**
- Delete `vehicle_positions` older than 10 minutes
- Delete `vehicle_arrivals` after aggregation
- Run PostgreSQL VACUUM to reclaim space

**Outcome:** Database remains <50MB indefinitely

---

### Authentication & Security

API key authentication with SHA-256 hashing protects sensitive endpoints while keeping general discovery endpoints public.

**How it works:**
- Admin generates API keys via a protected endpoint
- Keys are hashed with SHA-256 before storage — plain text keys are never persisted
- Protected endpoints verify incoming keys by hashing and comparing against the database
- Keys can be deactivated or deleted via admin endpoints

**Endpoint access:**
- **Public:** `/stops/`, `/routes/`, `/dwell-time/stats`, `/dwell-time/filters`, `/dwell-time/hotspots`
- **Protected:** `/vehicles/live`, `/dwell-time/routes`, `/dwell-time/route/{route}/stops`, `/dwell-time/stop/{id}/pattern`, `/dwell-time/heatmap`

Authentication is via `PTAnalytics-API-Key` header.

---

### Request Logging & Rate Limiting

All requests are logged with method, path, response time, and status code using structured Python logging. Errors include full tracebacks for production debugging.

Rate limiting via `slowapi` prevents abuse:
- **Global default:** 30 requests/minute per client IP
- **Per-endpoint overrides:** Expensive queries (e.g., `/dwell-time/hotspots`) are limited to 5 requests/minute
- Returns `429 Too Many Requests` when exceeded

---

### Database Schema

**Operational Tables (10-min retention):**
```sql
CREATE TABLE vehicle_positions (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT,
    route_name TEXT,
    direction TEXT,
    operator TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TIMESTAMP,
    analyzed BOOLEAN DEFAULT false
);
```

**Analytics Table (permanent, fixed-size):**
```sql
CREATE TABLE dwell_time_analysis (
    naptan_id VARCHAR(20),
    route_name VARCHAR(20),
    direction VARCHAR(20),
    operator VARCHAR(50),
    day_of_week INTEGER,      -- 0=Monday, 6=Sunday
    hour_of_day INTEGER,      -- 0-23
    avg_dwell_seconds REAL,
    stddev_dwell_seconds REAL,
    sample_count INTEGER,
    last_updated TIMESTAMP,
    PRIMARY KEY (naptan_id, route_name, direction, operator, day_of_week, hour_of_day)
);

CREATE INDEX idx_high_demand ON dwell_time_analysis(avg_dwell_seconds DESC);
```

**Auth Table:**
```sql
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL UNIQUE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    hashvalue VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Storage Strategy:**
- Raw data: Aggressive cleanup (10-min window)
- Aggregates: Permanent retention (running averages)
- Static data: One-time load from TransXChange files

---

## API Endpoints

**Base URL:** `https://api.heyico.work`

**Authentication:** Protected endpoints require `PTAnalytics-API-Key` header. Public endpoints are accessible without authentication.

### Stops & Routes (Public)

```http
GET /stops/?search=station&limit=100&offset=0
GET /stops/{stop_id}
GET /routes/?search=86&limit=100&offset=0
GET /routes/{route_name}
```

Paginated responses with `total`, `limit`, `offset`, `next`, `prev`, and `data` fields.

**Example — Stop Details:**
```json
{
  "stop": {
    "stop_id": "2800S40020D",
    "stop_name": "Queen Square Bus Station",
    "latitude": 53.4094,
    "longitude": -2.9886
  },
  "route": [
    {
      "route_name": "14",
      "operator_name": "Arriva Merseyside",
      "direction": "inbound"
    }
  ],
  "route_count": 15
}
```

---

### Live Vehicles (Protected)

```http
GET /vehicles/live?search=AMSY&limit=100&offset=0
```

Real-time vehicle positions within Liverpool bounding box (last 2 minutes). Search by operator code (e.g., AMSY for Arriva Merseyside).

**Response:**
```json
{
  "total": 42,
  "limit": 100,
  "offset": 0,
  "next": null,
  "prev": null,
  "data": [
    {
      "vehicle_id": "AMSY-1234",
      "latitude": 53.4084,
      "longitude": -2.9876,
      "bearing": 180.0,
      "timestamp": "2026-03-14T10:30:00",
      "route_name": "14",
      "direction": "inbound",
      "operator": "AMSY",
      "origin": "Liverpool",
      "destination": "Croxteth"
    }
  ]
}
```

---

### Dwell Time Analytics (Mixed)

**Public:**
```http
GET /dwell-time/stats
GET /dwell-time/filters
GET /dwell-time/hotspots?min_samples=10&limit=20
```

**Protected:**
```http
GET /dwell-time/routes?search=14&limit=100&offset=0
GET /dwell-time/route/{route_name}/stops?direction=inbound&operator=Arriva&day_of_week=1&hour_of_day=8
GET /dwell-time/stop/{naptan_id}/pattern?route_name=14
GET /dwell-time/heatmap?route_name=14&direction=inbound&operator=Arriva%20Merseyside
```

**Example — High-Demand Stops:**
```json
{
  "hotspots": [
    {
      "naptan_id": "2800S40020D",
      "stop_name": "Queen Square Bus Station",
      "latitude": 53.4094,
      "longitude": -2.9886,
      "routes_count": 15,
      "overall_avg_dwell": 45.2,
      "total_samples": 1247
    }
  ],
  "count": 20
}
```

**Example — Heatmap (stop × hour matrix):**
```json
{
  "route_name": "14",
  "direction": "inbound",
  "operator": "Arriva Merseyside",
  "stops": ["Stop A", "Stop B", "Stop C"],
  "hours": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
  "data": [
    [19.1, 22.5, 25.3, null, null, 15.2, 28.4, 42.1, 55.3, 48.7, 35.2, 30.1, 28.9, 27.5, 29.3, 35.8, 48.2, 52.1, 40.3, 32.1, 25.4, 20.1, 18.3, 17.2],
    [15.6, 17.6, 18.7, null, null, 12.1, 22.3, 38.9, 47.2, 41.3, 30.5, 26.8, 24.1, 23.7, 25.2, 31.4, 42.1, 46.8, 35.7, 28.4, 22.1, 17.8, 15.9, 14.2]
  ]
}
```

**Example — Paginated Routes with Dwell Data:**
```json
{
  "total": 137,
  "limit": 100,
  "offset": 0,
  "next": "https://api.heyico.work/dwell-time/routes?limit=100&offset=100",
  "prev": null,
  "data": [
    {
      "route_name": "14",
      "stops_with_data": 32,
      "operators": 2,
      "total_samples": 4601,
      "avg_dwell": 28.9
    }
  ]
}
```

---

### Admin (Protected)

Admin endpoints are hidden from Swagger documentation and require a separate admin password via `PTAnalytics-Admin-Password` header.

```http
GET  /admin/stats
POST /admin/create_api_key         {"user_name": "client_name"}
PATCH /admin/api-keys/{user_name}/deactivate
PATCH /admin/api-keys/{user_name}/activate
DELETE /admin/api-keys/{user_name}/delete
```

---

**Interactive Documentation:** [api.heyico.work/docs](https://api.heyico.work/docs) (OpenAPI/Swagger)

---

## Key Engineering Decisions

### Fixed-Size Analytics Table

**Challenge:** Unbounded data growth on free-tier storage constraints (500MB limit initially)

**Solution:** Running average aggregation with conflict resolution eliminates need for raw historical data

**Implementation:** SQL `ON CONFLICT DO UPDATE` merges new samples into existing averages

**Result:** Table size remains constant (~500K rows) regardless of months/years of operation

**Alternative Considered:** Time-series database (rejected due to complexity and resource overhead)

---

### In-Memory Spatial Matching

**Challenge:** Spatial queries matching 100+ stop events to 6,395 stops via database = 10+ minutes per cycle

**Solution:** Load stop topology into Python dictionary at startup, indexed by route and direction

**Result:** 100x performance improvement (10 minutes → 5 seconds per analysis cycle)

**Tradeoff:** 50MB memory footprint vs 10 minutes of CPU time per cycle

---

### 10-Minute Rolling Window

**Challenge:** 1GB RAM constraint on free-tier VM requires minimising database size

**Solution:** Aggressive cleanup of raw positions after 10 minutes, keeping only aggregated results

**Result:** Database stable at <50MB with full analytical capability preserved

**Alternative Considered:** Longer retention windows (rejected due to RAM constraints during PostgreSQL operations)

---

### TransXChange Integration

**Challenge:** UK transit data uses TransXChange (native) vs GTFS (international standard)

**Solution:** Built XML parser for TransXChange files, loaded 6,395 stops and 556 route patterns

**Result:** Native data format eliminates reconciliation issues common with GTFS conversions

**Learning:** Domain research revealed UK transit industry's data standards differ from international norms

---

## Performance Metrics

**Pipeline Efficiency:**
- 450 positions/minute ingested
- <5 seconds analysis latency for 100+ events
- <100ms API response time (p95)
- 99%+ uptime via systemd auto-restart

**Resource Utilisation:**
- 1GB RAM constraint met on both VMs
- <50MB database size maintained
- Zero external service dependencies
- $0/month operational cost

**Data Coverage:**
- 100+ routes monitored
- 11 transit operators
- 50,000+ dwell time samples collected
- 1,000+ stops with demand data

---

## Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Database** | PostgreSQL 14 + PostGIS | Geospatial queries, ACID compliance, free |
| **Backend** | Python 3.10 + FastAPI | Async I/O, type hints, auto-generated docs |
| **Async DB** | asyncpg | Native async PostgreSQL driver with connection pooling |
| **Validation** | Pydantic | Request/response validation, serialisation |
| **Auth** | API key (SHA-256 hashed) | Stateless, lightweight, suitable for service-to-service |
| **Rate Limiting** | slowapi | Per-IP and per-endpoint request throttling |
| **Testing** | pytest + httpx + TestClient | 28 tests covering auth, pagination, edge cases |
| **Orchestration** | Cron + systemd | Zero dependencies, reliable, lightweight |
| **Infrastructure** | Oracle Cloud (Always Free) | 2× VMs with 1GB RAM each, ARM architecture |
| **SSL/TLS** | Let's Encrypt + nginx | Free certificates with auto-renewal |
| **Data Source** | UK Bus Open Data Service | Government API, real-time GPS, free access |

**Key Libraries:** `asyncpg`, `fastapi`, `pydantic`, `slowapi`, `lxml`, `shapely`, `numpy`

---

## Testing

28 automated tests covering:
- **Public endpoints:** Stops and routes listing, search, pagination, detail views
- **Protected endpoints:** API key validation (403 missing, 401 invalid, 200 valid)
- **Edge cases:** Invalid IDs (404), invalid pagination (422), non-existent resources
- **Admin lifecycle:** Key creation, deactivation, double-deactivation guard, deletion

Run tests:
```bash
python -m pytest tests/test_api.py -v
```

---

## Challenges & Solutions

### Challenge 1: Storage Growth
**Initial Approach:** Store all raw vehicle positions for historical analysis

**Problem:** 1.8GB accumulated in 2 weeks, exceeding Supabase free tier (500MB)

**Solution:** Migrated to self-hosted PostgreSQL with 10-minute rolling window

**Outcome:** Database size stable at <50MB with full analytical capability

---

### Challenge 2: Spatial Query Performance
**Initial Approach:** Database spatial queries for each stop event match

**Problem:** 10+ minutes per analysis cycle with 100+ events

**Solution:** Load stop topology into memory once, perform matching in application layer

**Outcome:** 100x speedup, 5-second analysis cycles

---

### Challenge 3: RAM Constraints
**Challenge:** 1GB RAM insufficient for holding full dataset

**Solution:** Streaming processing, indexed queries, aggressive cleanup

**Outcome:** Demonstrated optimisation for constrained environments

---

### Challenge 4: UK Data Standards
**Initial Approach:** GTFS Static + GTFS-RT (international standard)

**Problem:** 3.4% trip_id match rate in UK data, only 37 stops operational

**Solution:** Pivoted to TransXChange (UK native format), 6.5x stop coverage improvement

**Outcome:** 6,395 stops loaded, 98% theoretical match capability

---

## Dwell Time Methodology

### Academic Basis

**Research Foundation:**
- Tirachini (2013): Dwell time positively correlates with passenger load in bus systems
- Transit Capacity and Quality of Service Manual (TCRP): Standard metric for demand estimation
- Used by: Transport for London, MBTA Boston, WMATA Washington DC

**Validation Approach:**
- Compare patterns with known high-demand locations (stadiums, universities, hospitals)
- Verify peak vs off-peak hourly patterns align with commuter behaviour
- Check directional asymmetry (inbound morning rush, outbound evening rush)

---

### Advantages Over Traditional Methods

**Traditional:** Manual passenger counts, expensive APC (Automatic Passenger Counter) hardware

**Dwell Time Approach:**
- No additional hardware beyond existing GPS
- Real-time updates vs quarterly surveys
- City-wide coverage vs sample-based counts
- Continuous 24/7 data collection

---

### Known Limitations

- Cannot distinguish boarding vs alighting activity
- Weather and traffic incidents affect accuracy
- Assumes constant boarding/alighting speed across all stops
- Does not capture denied boardings (bus too full)

---

## Project Evolution

**November 2025:** Initial development with GTFS Static approach
→ Abandoned due to data quality issues in UK (3.4% match rate)

**December 2025:** Migrated to TransXChange (UK native format)
→ 6.5x improvement in stop coverage (37 → 240 stops)

**December 2025:** Pivoted from complex Service Reliability Index to focused dwell time analysis
→ Simplified metric with clearer business value

**December 2025 - January 2026:** Production hardening
→ SSL deployment, systemd management, aggressive optimisation for 1GB RAM constraints

**March 2026:** API modernisation
→ Async endpoints (asyncpg), Pydantic models, API key auth, rate limiting, 28 automated tests

---

## Use Cases

### 1. Infrastructure Planning
**Question:** Where should investment go for bus shelter upgrades?

**Solution:** Query `/dwell-time/hotspots` to identify top 20 highest-demand stops

**Impact:** Data-driven capital investment decisions

---

### 2. Schedule Optimisation
**Question:** Should Route 14 increase frequency during morning peak?

**Solution:** Query `/dwell-time/heatmap` to analyse hourly demand patterns

**Impact:** Right-sized service levels, reduced overcrowding

---

### 3. Operator Benchmarking
**Question:** How does Arriva's performance compare to Stagecoach on Route 14?

**Solution:** Filter `/dwell-time/routes` by operator

**Impact:** Performance-based service contracts

---

### 4. Demand-Responsive Transit
**Question:** When should on-demand services operate in low-demand areas?

**Solution:** Identify time periods with low average dwell times

**Impact:** Cost-effective hybrid fixed-route/on-demand systems

---

## Setup Requirements

**Infrastructure:**
- 2× Linux VMs with 1GB RAM each (Oracle Cloud Always Free tier)
- PostgreSQL 14 + PostGIS
- Python 3.10+
- nginx + Let's Encrypt for SSL

**Data Sources:**
- UK Bus Open Data Service API key (free registration)
- TransXChange XML timetable files for target region

**Deployment:**
- FastAPI as systemd service
- Cron-scheduled data pipeline
- nginx reverse proxy with SSL termination

*Note: Detailed deployment procedures are proprietary*

---

## Contact

**API Demo:** [api.heyico.work/docs](https://api.heyico.work/docs)
**GitHub:** [josephmjustin/pt-analytics](https://github.com/josephmjustin/pt-analytics)
**LinkedIn:** [josephmjustin](https://www.linkedin.com/in/josephmjustin/)

---

**Real-time demand analytics for public transport. Zero operational cost. Production-ready architecture.**