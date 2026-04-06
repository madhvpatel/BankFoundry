from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal

Category = Literal["performance", "growth", "risk", "reconciliation", "disputes"]


@dataclass
class EvidencePreview:
    columns: List[str]
    rows: List[List[Any]]
    row_count: int


@dataclass
class Recommendation:
    reco_id: str
    merchant_id: str
    window_days: int
    category: Category
    title: str
    summary: str
    impact_rupees: float
    confidence: float
    priority_score: float
    drivers: List[Dict[str, Any]] = field(default_factory=list)
    actions: List[Dict[str, str]] = field(default_factory=list)
    evidence_ids: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
