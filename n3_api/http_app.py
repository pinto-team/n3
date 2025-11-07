# File: noema/n3_api/http_app.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from n3_api.routes import policy, knowledge, skills, chat, initiative, ws, ui

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
    return {"ok": True, "name": "noema-dev-api"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("noema.n3_api.http_app:app", host="0.0.0.0", port=8080, reload=True)
