# Folder: noema/tests/e2e
# File:   test_minimal_chat.py

from fastapi.testclient import TestClient
from n3_api.http_app import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("ok") is True

def test_chat_e2e_emit_contains_user_text():
    payload = {"thread_id": "t-e2e", "text": "hello from test"}
    r = client.post("/chat", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    emitted = data["emitted"]
    assert isinstance(emitted, list) and len(emitted) >= 1
    # Message text should contain the echoed user text (JSON string from echo skill)
    text = emitted[-1].get("text", "")
    assert "hello from test" in text

def test_skills_exec_aggregate():
    payload = {
        "thread_id": "t-e2e",
        "calls": [
            {"req_id": "r-agg", "skill_id": "skill.dev.echo", "params": {"k": "v"}}
        ],
    }
    r = client.post("/skills", json=payload)
    assert r.status_code == 200
    agg = r.json().get("aggregate", {})
    # Expect at least one call executed and aggregated
    assert agg.get("count", 0) >= 1
    assert agg.get("avg_latency_ms", 0) >= 0

def test_tick_snapshot_progress():
    # Ensure session exists
    client.post("/tick", json={"thread_id": "t-snap"})
    # Add a skill call then run another tick
    client.post("/skills", json={
        "thread_id": "t-snap",
        "calls": [{"req_id":"r1","skill_id":"skill.dev.echo","params":{"msg":"snap"}}]
    })
    r = client.post("/tick", json={"thread_id": "t-snap"})
    assert r.status_code == 200
    snap = r.json()["snapshot"]
    assert "keys" in snap and isinstance(snap["keys"], list)
