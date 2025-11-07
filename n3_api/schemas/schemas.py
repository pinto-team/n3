# Folder: noema/n3_api
# File:   schemas.py

from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field

# Append to the bottom of schemas.py (no Persian comments)

class PolicyChange(BaseModel):
    path: str
    new_value: Any
    change_type: str = "set"
    rationale: Optional[str] = None
    confidence: Optional[float] = None

class PolicyApplyRequest(BaseModel):
    thread_id: str
    changes: List[PolicyChange]

class IngestRequest(BaseModel):
    thread_id: str
    doc_id: str
    text: str

class SkillCall(BaseModel):
    req_id: str
    skill_id: str
    params: Dict[str, Any] = Field(default_factory=dict)

class TickRequest(BaseModel):
    thread_id: str

class TickReport(BaseModel):
    ran: List[str] = Field(default_factory=list)
    skipped: List[str] = Field(default_factory=list)
    errors: List[Dict[str, Any]] = Field(default_factory=list)

class ChatRequest(BaseModel):
    thread_id: str
    text: str

class SkillsRequest(BaseModel):
    thread_id: str
    calls: List[SkillCall]

class StateSnapshot(BaseModel):
    keys: List[str]
    has_executor_results: bool = False
    has_transport_outbound: bool = False

class OutboxResponse(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)

class InitiativeItem(BaseModel):
    id: str
    type: Literal["say", "run_skill"]
    when_ms: Optional[int] = None
    in_ms: Optional[int] = 0
    once: bool = True
    cooldown_ms: int = 0
    payload: Dict[str, Any] = Field(default_factory=dict)

class InitiativeAddRequest(BaseModel):
    thread_id: str
    items: List[InitiativeItem]