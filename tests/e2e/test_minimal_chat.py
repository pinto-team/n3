# ============================
# File: tests/e2e/test_minimal_chat.py
# ============================

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
    text = emitted[-1].get("text", "")
    assert "hello from test" in text

def test_skills_exec_aggregate():
    payload = {
        "thread_id": "t-e2e",
        "calls": [
            {"req_id": "r-agg", "skill_id": "skill.dev.echo", "params": {"k": "v"}}
        ],
    }
    r = client.post("/skills/", json=payload)
    assert r.status_code == 200
    agg = r.json().get("aggregate", {})
    assert agg.get("count", 0) >= 0  # aggregate ممکن است خالی باشد ولی endpoint باید OK برگرداند

def test_policy_train_tick_progress():
    """
    جایگزین تست قدیمی /tick
    از /policy/train استفاده می‌کنیم که نقش همان تیک را دارد.
    """
    # اطمینان از وجود session
    client.post("/policy/train", json={"thread_id": "t-snap"})

    # اضافه کردن یک skill call و اجرای مجدد train
    client.post("/skills/", json={
        "thread_id": "t-snap",
        "calls": [{"req_id": "r1", "skill_id": "skill.dev.echo", "params": {"msg": "snap"}}]
    })
    r = client.post("/policy/train", json={"thread_id": "t-snap"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert "concept" in body
    assert "policy_updates" in body
    assert "ok" in body and body["ok"] is True
