import base64
import json
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence


@dataclass
class ApexReading:
    name: str
    value: float
    unit: str = ""
    timestamp: Optional[str] = None


@dataclass
class ApexClient:
    host: str
    username: str | None = None
    password: str | None = None
    api_token: str | None = None
    timeout: int = 10

    def fetch_readings(self) -> List[ApexReading]:
        if not self.host:
            raise ValueError("Apex host is required")
        payload = self._fetch_payload()
        probes = _extract_probe_list(payload)
        readings: List[ApexReading] = []
        for probe in probes:
            reading = _parse_probe(probe)
            if reading:
                readings.append(reading)
        if not readings:
            raise ValueError("No probe readings found in Apex response")
        return readings

    def _fetch_payload(self) -> Any:
        urls = _build_urls(self.host)
        headers = {"Accept": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        elif self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("utf-8")
            headers["Authorization"] = f"Basic {token}"
        last_error: Optional[Exception] = None
        for url in urls:
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    content = resp.read().decode("utf-8")
                return _parse_payload(content)
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    last_error = ValueError(
                        "Apex authentication failed (401). Check username/password or API token."
                    )
                else:
                    last_error = exc
            except urllib.error.URLError as exc:
                if getattr(getattr(exc, "reason", None), "errno", None) == 113:
                    last_error = ValueError(
                        "Unable to reach Apex host. Verify the IP/port and that this server can reach it on the network."
                    )
                else:
                    last_error = exc
            except Exception as exc:
                last_error = exc
        if last_error:
            raise last_error
        raise RuntimeError("Unable to fetch Apex payload")


def _extract_probe_list(payload: Any) -> Sequence[Dict[str, Any]]:
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("probes", "probe", "items", "data"):
        val = payload.get(key)
        if isinstance(val, list):
            return [p for p in val if isinstance(p, dict)]
        if isinstance(val, dict):
            nested = _extract_probe_list(val)
            if nested:
                return nested
    for key in ("status", "response", "result"):
        val = payload.get(key)
        if isinstance(val, dict):
            nested = _extract_probe_list(val)
            if nested:
                return nested
    return []


def _parse_probe(probe: Dict[str, Any]) -> Optional[ApexReading]:
    name = _first_value(probe, ["name", "probe", "label", "title", "type"])
    if not name:
        return None
    raw_value = _first_value(probe, ["value", "reading", "state", "current", "last"]) 
    value = _to_float(raw_value)
    if value is None:
        return None
    unit = _first_value(probe, ["unit", "units", "uom", "measure"]) or ""
    timestamp = _first_value(probe, ["timestamp", "time", "updated_at", "updated", "last_updated"]) 
    return ApexReading(name=str(name), value=value, unit=str(unit), timestamp=str(timestamp) if timestamp else None)


def _first_value(data: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return None


def _to_float(value: Any) -> Optional[float]:
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


def _build_urls(host: str) -> List[str]:
    trimmed = host.strip()
    if not trimmed:
        return []
    endpoints = ["/cgi-bin/status.xml", "/rest/status", "/rest/status.json", "/rest/"]
    if "://" in trimmed:
        split = urllib.parse.urlsplit(trimmed)
        netloc = split.netloc or split.path.split("/")[0]
        if not netloc:
            return []
        if split.path and split.path != "/":
            return [urllib.parse.urlunsplit((split.scheme, netloc, split.path, split.query, ""))]
        return [f"{split.scheme}://{netloc}{endpoint}" for endpoint in endpoints]
    if "/" in trimmed:
        host_only, path = trimmed.split("/", 1)
        if not host_only:
            return []
        path = f"/{path}"
        return [f"http://{host_only}{path}", f"https://{host_only}{path}"]
    return [f"http://{trimmed}{endpoint}" for endpoint in endpoints] + [
        f"https://{trimmed}{endpoint}" for endpoint in endpoints
    ]


def _parse_payload(content: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return _parse_xml_payload(content)


def _parse_xml_payload(content: str) -> Dict[str, Any]:
    root = ET.fromstring(content)
    probes: List[Dict[str, Any]] = []
    for elem in root.iter():
        attrib = elem.attrib or {}
        name = _first_value(attrib, ["name", "probe", "label", "title", "type"])
        value = _first_value(attrib, ["value", "reading", "state", "current", "val"])
        if name is None or value is None:
            continue
        probes.append(
            {
                "name": name,
                "value": value,
                "unit": _first_value(attrib, ["unit", "units", "uom", "measure"]) or "",
                "timestamp": _first_value(attrib, ["timestamp", "time", "updated_at", "updated", "last_updated"]),
            }
        )
    if not probes:
        raise ValueError("No probe readings found in Apex XML response.")
    return {"probes": probes}
