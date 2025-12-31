from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        try:
            return float(text.replace(",", "."))
        except Exception:
            return None


def parse_conversion_table(
    text: str,
    to_float: Callable[[Any], Optional[float]] = coerce_float,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    if not text:
        return rows
    cleaned = text.replace("\t", " ")
    if "\n" not in cleaned and "," in cleaned:
        cleaned = cleaned.replace(" ", "\n")
    for line in cleaned.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 2:
            continue
        remaining = to_float(parts[0])
        value = to_float(parts[1])
        if remaining is None or value is None:
            continue
        rows.append({"remaining": float(remaining), "value": float(value)})
    return rows


def build_test_kit_payload(
    *,
    parameter: Optional[str],
    name: str,
    unit: Optional[str],
    resolution: Optional[str],
    min_value: Optional[str],
    max_value: Optional[str],
    notes: Optional[str],
    conversion_type: Optional[str],
    conversion_table: Optional[str],
    active: Optional[str],
    to_float: Callable[[Any], Optional[float]] = coerce_float,
) -> Dict[str, Any]:
    is_active = 1 if active in ("1", "on", "true", "True") else 0
    conv_type = (conversion_type or "").strip() or None
    conversion_rows = parse_conversion_table(conversion_table or "", to_float) if conv_type else []
    conversion_json = json.dumps(conversion_rows) if conversion_rows else None
    return {
        "parameter": (parameter or "").strip(),
        "name": name.strip(),
        "unit": (unit or "").strip() or None,
        "resolution": to_float(resolution),
        "min_value": to_float(min_value),
        "max_value": to_float(max_value),
        "notes": (notes or "").strip() or None,
        "conversion_type": conv_type,
        "conversion_data": conversion_json,
        "active": is_active,
    }
