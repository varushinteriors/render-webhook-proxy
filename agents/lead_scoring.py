from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass
class LeadInput:
    timeline: str | None = None
    budget: str | None = None
    property_type: str | None = None
    service_type: str | None = None
    assets_shared: bool = False
    answered_fields: int = 0
    total_fields: int = 0


class LeadScoringAgent:
    """Classifies leads as cold / warm / hot with reason codes."""

    HOT_TIMELINES = {"immediately", "within 3 months"}
    MID_TIMELINES = {"within 6 months", "within three months", "within six months"}
    HIGH_VALUE_PROPERTIES = {"3 bhk", "4 bhk", "5 bhk", "independent", "villa", "farmhouse"}

    def score(self, lead: LeadInput) -> Dict[str, Any]:
        timeline = self._normalize(lead.timeline)
        budget_level = self._budget_level(lead.budget)
        property_value = self._property_value(lead.property_type)
        answered_all = lead.total_fields > 0 and lead.answered_fields >= lead.total_fields

        reasons: List[str] = []
        label = "cold"

        # Hot scenarios
        if timeline in self.HOT_TIMELINES and budget_level in {"high", "flex"}:
            label = "hot"
            reasons.append("Fast timeline with high/flexible budget")
        elif timeline in self.HOT_TIMELINES and property_value == "high":
            label = "hot"
            reasons.append("Fast timeline with high-value property")
        elif lead.assets_shared and answered_all:
            label = "hot"
            reasons.append("Shared layouts/photos and completed all answers")
        else:
            # Warm vs cold
            if timeline in self.HOT_TIMELINES or timeline in self.MID_TIMELINES:
                label = "warm"
                reasons.append("Moderate timeline")
            if budget_level in {"mid", "high", "flex"}:
                if label == "cold":
                    label = "warm"
                reasons.append("Viable budget range")
            if label == "cold":
                reasons.append("Long/unknown timeline or low budget")

        return {
            "score": label,
            "reasons": reasons,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "features": {
                "timeline": lead.timeline,
                "budget": lead.budget,
                "property_type": lead.property_type,
                "assets_shared": lead.assets_shared,
                "answered_fields": lead.answered_fields,
                "total_fields": lead.total_fields,
            },
        }

    def _normalize(self, value: str | None) -> str | None:
        if not value:
            return None
        return value.strip().lower()

    def _budget_level(self, value: str | None) -> str:
        if not value:
            return "unknown"
        val = value.lower()
        if "flex" in val or "per design" in val:
            return "flex"
        if any(tok in val for tok in ["30", "40", "50", "above"]):
            return "high"
        if any(tok in val for tok in ["20", "25"]):
            return "mid"
        if any(tok in val for tok in ["10", "15"]):
            return "low"
        return "unknown"

    def _property_value(self, value: str | None) -> str:
        if not value:
            return "unknown"
        val = value.lower()
        if any(token in val for token in self.HIGH_VALUE_PROPERTIES):
            return "high"
        return "standard"
