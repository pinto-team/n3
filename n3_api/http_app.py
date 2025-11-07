# File: noema/n3_api/http_app.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from n3_api.routes import policy, knowledge, skills, chat, initiative, ws, ui
from n3_api.utils.state import ensure_state, get_sessions

app = FastAPI(title="Noema Dev API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(policy.router)
app.include_router(knowledge.router)
app.include_router(skills.router)
app.include_router(chat.router)
app.include_router(initiative.router)
app.include_router(ws.router)
app.include_router(ui.router)

@app.get("/health")
def health():
    sessions = get_sessions()
    summary = []
    for tid, state in sessions.items():
        obs = (state.get("observability") or {}).get("telemetry", {})
        summ = obs.get("summary", {}) if isinstance(obs.get("summary"), dict) else {}
        adaptation = (state.get("adaptation") or {}).get("policy", {}) if isinstance(state.get("adaptation"), dict) else {}
        concept = (state.get("concept_graph") or {}).get("version", {}) if isinstance(state.get("concept_graph"), dict) else {}
        summary.append({
            "thread_id": tid,
            "uncertainty": summ.get("uncertainty"),
            "policy_updates": adaptation.get("updates"),
            "avg_reward": adaptation.get("avg_reward"),
            "concept_version": concept.get("id"),
        })
    return {"ok": True, "name": "noema-dev-api", "sessions": summary}


@app.get("/introspect/{thread_id}")
def introspect(thread_id: str):
    state = ensure_state(thread_id)
    adaptation = (state.get("adaptation") or {}).get("policy", {}) if isinstance(state.get("adaptation"), dict) else {}
    concept = state.get("concept_graph") if isinstance(state.get("concept_graph"), dict) else {}
    telemetry = (state.get("observability") or {}).get("telemetry", {}) if isinstance(state.get("observability"), dict) else {}
    return {
        "thread_id": thread_id,
        "adaptation": adaptation,
        "concept": {
            "version": (concept.get("version") or {}),
            "updates": concept.get("updates") if isinstance(concept.get("updates"), dict) else {},
        },
        "telemetry": telemetry,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("noema.n3_api.http_app:app", host="0.0.0.0", port=8080, reload=True)
