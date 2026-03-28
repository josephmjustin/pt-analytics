import pytest
import os
from fastapi.testclient import TestClient
from src.api.main import app

API_KEY = os.getenv("PTAnalytics_API_KEY")
ADMIN_KEY = os.getenv("ADMIN_PASSWORD")

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

# ==================== ROOT ====================

def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["version"] == "1.0.0"
    assert "features" in data

# ==================== STOPS (public) ====================

def test_get_stops(client):
    response = client.get("/stops/")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "data" in data
    assert isinstance(data["data"], list)
    assert data["limit"] == 100  # default
    assert data["offset"] == 0  # default

def test_get_stops_pagination(client):
    response = client.get("/stops/?limit=5&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) <= 5
    assert data["limit"] == 5

def test_get_stops_search(client):
    response = client.get("/stops/?search=station")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data["data"], list)

def test_get_stops_invalid_pagination(client):
    response = client.get("/stops/?limit=-1")
    assert response.status_code == 422

def test_get_stop_by_id(client):
    # First get a valid stop ID
    list_response = client.get("/stops/?limit=1")
    stops = list_response.json()["data"]
    if stops:
        stop_id = stops[0]["stop_id"]
        response = client.get(f"/stops/{stop_id}")
        assert response.status_code == 200
        data = response.json()
        assert "stop" in data
        assert "route" in data
        assert "route_count" in data
        assert data["stop"]["stop_id"] == stop_id

def test_get_stop_invalid_id(client):
    response = client.get("/stops/FAKE_STOP_ID_999")
    assert response.status_code == 404

# ==================== ROUTES (public) ====================

def test_get_routes(client):
    response = client.get("/routes/")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "data" in data
    assert isinstance(data["data"], list)

def test_get_routes_search(client):
    response = client.get("/routes/?search=86")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data["data"], list)

def test_get_route_by_name(client):
    # Get a valid route name first
    list_response = client.get("/routes/?limit=1")
    routes = list_response.json()["data"]
    if routes:
        route_name = routes[0]["route_name"]
        response = client.get(f"/routes/{route_name}")
        assert response.status_code == 200
        data = response.json()
        assert "variants" in data
        assert "stops_in_sequence" in data
        assert data["route_name"] == route_name

def test_get_route_invalid_name(client):
    response = client.get("/routes/FAKE_ROUTE_999")
    assert response.status_code == 404

# ==================== VEHICLES (protected) ====================

def test_vehicles_no_auth(client):
    response = client.get("/vehicles/live")
    assert response.status_code == 403

def test_vehicles_wrong_key(client):
    response = client.get("/vehicles/live", headers={"PTAnalytics-API-Key": "wrong-key"})
    assert response.status_code == 401

def test_vehicles_valid_key(client):
    response = client.get("/vehicles/live", headers={"PTAnalytics-API-Key": API_KEY})
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "data" in data
    assert isinstance(data["data"], list)

# ==================== DWELL TIME (mixed auth) ====================

def test_dwell_time_stats(client):
    response = client.get("/dwell-time/stats")
    assert response.status_code == 200
    data = response.json()
    assert "unique_stops" in data
    assert "total_samples" in data
    assert data["total_samples"] > 0

def test_dwell_time_filters(client):
    response = client.get("/dwell-time/filters")
    assert response.status_code == 200
    data = response.json()
    assert "operators" in data
    assert "directions" in data
    assert isinstance(data["operators"], list)

def test_dwell_routes_no_auth(client):
    response = client.get("/dwell-time/routes")
    assert response.status_code == 403

def test_dwell_routes_valid_key(client):
    response = client.get("/dwell-time/routes", headers={"PTAnalytics-API-Key": API_KEY})
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "data" in data

def test_dwell_route_stops(client):
    # Get a valid route first
    routes_response = client.get("/dwell-time/routes?limit=1", headers={"PTAnalytics-API-Key": API_KEY})
    routes = routes_response.json()["data"]
    if routes:
        route_name = routes[0]["route_name"]
        response = client.get(
            f"/dwell-time/route/{route_name}/stops",
            headers={"PTAnalytics-API-Key": API_KEY}
        )
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "data" in data

def test_dwell_stop_pattern_invalid(client):
    response = client.get(
        "/dwell-time/stop/FAKE_STOP/pattern",
        headers={"PTAnalytics-API-Key": API_KEY}
    )
    assert response.status_code == 404

def test_dwell_hotspots(client):
    response = client.get("/dwell-time/hotspots")
    assert response.status_code == 200
    data = response.json()
    assert "hotspots" in data
    assert "count" in data
    assert isinstance(data["hotspots"], list)

def test_dwell_heatmap_no_auth(client):
    response = client.get("/dwell-time/heatmap?route_name=86")
    assert response.status_code == 403

def test_dwell_heatmap_valid(client):
    # Get a valid route first
    routes_response = client.get("/dwell-time/routes?limit=1", headers={"PTAnalytics-API-Key": API_KEY})
    routes = routes_response.json()["data"]
    if routes:
        route_name = routes[0]["route_name"]
        response = client.get(
            f"/dwell-time/heatmap?route_name={route_name}",
            headers={"PTAnalytics-API-Key": API_KEY}
        )
        assert response.status_code == 200
        data = response.json()
        assert "stops" in data
        assert "hours" in data
        assert "data" in data
        assert len(data["hours"]) == 24

# ==================== ADMIN (protected) ====================

def test_admin_no_auth(client):
    response = client.get("/admin/stats")
    assert response.status_code == 403

def test_admin_wrong_key(client):
    response = client.get("/admin/stats", headers={"PTAnalytics-Admin-Password": "wrong"})
    assert response.status_code == 401

def test_admin_stats(client):
    response = client.get("/admin/stats", headers={"PTAnalytics-Admin-Password": ADMIN_KEY})
    assert response.status_code == 200
    data = response.json()
    assert "total_users" in data
    assert "total_active_users" in data

def test_admin_create_and_deactivate_key(client):
    headers = {"PTAnalytics-Admin-Password": ADMIN_KEY}
    
    # Create
    create_response = client.post(
        "/admin/create_api_key",
        headers=headers,
        json={"user_name": "pytest_temp_user"}
    )
    assert create_response.status_code == 200
    assert "api_key" in create_response.json()

    # Deactivate
    deactivate_response = client.patch(
        "/admin/api-keys/pytest_temp_user/deactivate",
        headers=headers
    )
    assert deactivate_response.status_code == 200
    assert not deactivate_response.json()["active"]

    # Deactivate again — should fail
    again_response = client.patch(
        "/admin/api-keys/pytest_temp_user/deactivate",
        headers=headers
    )
    assert again_response.status_code == 400

    # Clean up — delete
    delete_response = client.delete(
        "/admin/api-keys/pytest_temp_user/delete",
        headers=headers
    )
    assert delete_response.status_code == 200

def test_admin_deactivate_nonexistent(client):
    response = client.patch(
        "/admin/api-keys/nonexistent_user/deactivate",
        headers={"PTAnalytics-Admin-Password": ADMIN_KEY}
    )
    assert response.status_code == 404