from __future__ import annotations

import json
import math
import re
import time
import http.cookiejar
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = ROOT / "tracked_items.json"
OUTPUT_PATH = ROOT / "prices.json"

ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
BRICKLINK_SET_PAGE_URL = "https://www.bricklink.com/v2/catalog/catalogitem.page?S={}"
BRICKLINK_MINIFIG_PAGE_URL = "https://www.bricklink.com/v2/catalog/catalogitem.page?M={}"
BRICKLINK_LISTING_AJAX_URL = "https://www.bricklink.com/ajax/clone/catalogifs.ajax"
BRICKLINK_CURRENCY_VARS_URL = "https://www.bricklink.com/js/allVars.js"
BRICKLINK_SEARCH_URL = "https://r.jina.ai/http://https://www.bricklink.com/v2/search.page?q={}"
R_JINA_PREFIX = "https://r.jina.ai/http://"

USER_AGENT = "PeekBrickStaticUpdater/1.0"
COOKIE_JAR = http.cookiejar.CookieJar()
OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(COOKIE_JAR))
BRICKLINK_ITEM_ID_PATTERN = re.compile(r"idItem\s*:\s*(\d+)", re.IGNORECASE)
BRICKLINK_CURRENCY_PATTERN = re.compile(
    r"idCurrency:\s*(\d+),\s*strCurrencyName:\s*'[^']*',\s*strCurrencyCode:\s*'([A-Z]{3})'"
)
BRICKLINK_MINIFIG_SEARCH_PATTERN = re.compile(
    r"\[([^\]]+)\]\(https://www\.bricklink\.com/v2/catalog/catalogitem\.page\?M=([A-Za-z0-9-]+)[^)]*\)",
    re.IGNORECASE,
)
MONEY_PATTERN = re.compile(r"\$([\d,]+(?:\.\d{2})?)")


@dataclass
class Candidate:
    code: str
    name: str
    score: float


def main() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    exchange_rates = fetch_exchange_rates()
    currency_map = fetch_bricklink_currency_map()

    set_entries: dict[str, Any] = {}
    set_aliases: dict[str, str] = {}

    for item in manifest.get("sets", []):
        entry = build_set_entry(item, exchange_rates, currency_map)
        if entry is None:
            continue
        key = item["setNumber"].strip().lower()
        set_entries[key] = entry
        set_aliases[key] = key
        primary = (item.get("primarySetNumber") or f"{item['setNumber']}-1").strip().lower()
        set_aliases[primary] = key

    minifig_entries: dict[str, Any] = {}
    minifigure_aliases: dict[str, str] = {}

    for item in manifest.get("minifigures", []):
        entry = build_minifigure_entry(item, exchange_rates, currency_map)
        if entry is None:
            continue
        key = (entry.get("brickLinkCode") or item["figureNumber"]).strip().lower()
        minifig_entries[key] = entry
        minifigure_aliases[item["figureNumber"].strip().lower()] = key
        minifigure_aliases[key] = key
        minifigure_aliases[slugify_name(item["name"])] = key
        for alias in item.get("aliases", []):
            minifigure_aliases[slugify_name(alias)] = key

    payload = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "sourceRepo": "https://github.com/Brujupeek1997/prices-repo",
        "hostedUrl": "https://brujupeek1997.github.io/prices-repo/prices.json",
        "setAliases": dict(sorted(set_aliases.items())),
        "minifigureAliases": dict(sorted(minifigure_aliases.items())),
        "sets": dict(sorted(set_entries.items())),
        "minifigures": dict(sorted(minifig_entries.items())),
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def build_set_entry(item: dict[str, Any], exchange_rates: dict[str, float], currency_map: dict[int, str]) -> dict[str, Any] | None:
    primary = item.get("primarySetNumber") or f"{item['setNumber']}-1"
    listings = fetch_bricklink_listings(BRICKLINK_SET_PAGE_URL.format(urllib.parse.quote(primary)), exchange_rates, currency_map)

    prices = {
        "sealed": build_set_condition(filter_set_bucket(listings, "N", "S")),
        "new_opened": build_set_condition(filter_set_bucket(listings, "N", "C")),
        "opened": build_set_condition(filter_set_bucket(listings, "U", "C")),
        "built": build_set_condition(filter_set_bucket(listings, "U", "B")),
    }
    prices = {key: value for key, value in prices.items() if value is not None}

    official_url = item.get("officialUrl")
    official = fetch_lego_snapshot(official_url) if official_url else None
    retail_price = official.get("priceUsd") if official else item.get("retailPriceUsd")
    availability = official.get("availability") if official else item.get("availability")

    market_mode = "live-model"
    if not prices:
        if official and official.get("priceUsd"):
            market_mode = "lego-store-official"
            prices = {
                "sealed": build_official_price_condition(official["priceUsd"]),
            }
        else:
            return None

    return {
        "setNumber": item["setNumber"],
        "primarySetNumber": primary,
        "name": item["name"],
        "year": item.get("year"),
        "pieces": item.get("pieces"),
        "image": item.get("image", ""),
        "theme": item.get("theme"),
        "subtheme": item.get("subtheme"),
        "retailPriceUsd": retail_price,
        "availability": availability,
        "retired": item.get("retired"),
        "marketMode": market_mode,
        "officialUrl": official_url,
        "updatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "sourceNote": (
            "Daily official LEGO Store snapshot for an upcoming set."
            if market_mode == "lego-store-official"
            else "Daily BrickLink arithmetic-average snapshot for a tracked popular set."
        ),
        "prices": prices,
    }


def build_minifigure_entry(item: dict[str, Any], exchange_rates: dict[str, float], currency_map: dict[int, str]) -> dict[str, Any] | None:
    code = item.get("brickLinkCode") or resolve_minifigure_code(item)
    if not code:
        return None
    listings = fetch_bricklink_listings(BRICKLINK_MINIFIG_PAGE_URL.format(urllib.parse.quote(code)), exchange_rates, currency_map)
    prices = {
        "new": build_minifigure_condition(filter_minifigure_bucket(listings, "N")),
        "used": build_minifigure_condition(filter_minifigure_bucket(listings, "U")),
    }
    prices = {key: value for key, value in prices.items() if value is not None}
    if not prices:
        return None
    return {
        "figureNumber": item["figureNumber"],
        "brickLinkCode": code,
        "name": item["name"],
        "image": item.get("image", ""),
        "pieces": item.get("pieces"),
        "marketMode": "live-model",
        "updatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "sourceNote": "Daily BrickLink arithmetic-average snapshot for a tracked popular minifigure.",
        "prices": prices,
    }


def resolve_minifigure_code(item: dict[str, Any]) -> str | None:
    target_name = item["name"]
    queries = [target_name] + item.get("aliases", []) + build_name_queries(target_name)
    seen: set[str] = set()
    best: Candidate | None = None
    for query in queries:
        normalized = query.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        markdown = fetch_text(BRICKLINK_SEARCH_URL.format(urllib.parse.quote(normalized)))
        if not markdown:
            continue
        for candidate in parse_minifigure_search_candidates(markdown, target_name):
            if best is None or candidate.score > best.score:
                best = candidate
        if best and best.score >= 0.78:
            break
        time.sleep(0.2)
    return best.code.lower() if best else None


def build_name_queries(name: str) -> list[str]:
    normalized = clean_text(re.sub(r"[()]+", " ", name))
    first_clause = clean_text(normalized.split(",")[0])
    tokens = re.findall(r"[a-z0-9]+", normalized.lower())
    variants = [
        normalized,
        " ".join(tokens),
        " ".join(tokens[:6]),
        " ".join(tokens[:5]),
        " ".join(tokens[:4]),
        first_clause,
        " ".join(tokens[:3]),
    ]
    result: list[str] = []
    for variant in variants:
        trimmed = clean_text(variant)
        if trimmed and trimmed not in result:
            result.append(trimmed)
    return result


def parse_minifigure_search_candidates(markdown: str, target_name: str) -> list[Candidate]:
    candidates: dict[str, Candidate] = {}
    for match in BRICKLINK_MINIFIG_SEARCH_PATTERN.finditer(markdown):
        name = clean_text(match.group(1))
        code = match.group(2).lower()
        score = similarity_score(target_name, name)
        current = candidates.get(code)
        if current is None or score > current.score:
            candidates[code] = Candidate(code=code, name=name, score=score)
    return sorted(candidates.values(), key=lambda candidate: candidate.score, reverse=True)


def fetch_bricklink_listings(item_page_url: str, exchange_rates: dict[str, float], currency_map: dict[int, str]) -> list[dict[str, Any]]:
    page_html = fetch_text(item_page_url)
    if not page_html:
        return []
    item_match = BRICKLINK_ITEM_ID_PATTERN.search(page_html)
    if not item_match:
        return []
    item_id = item_match.group(1)

    listings: list[dict[str, Any]] = []
    page = 1
    page_count = 1
    while page <= page_count:
        url = build_listing_url(item_id, page, 100)
        payload = fetch_json(url, referer=item_page_url)
        if not payload:
            break
        total_count = int(payload.get("total_count") or 0)
        page_count = max(1, math.ceil(total_count / 100))
        page_listings = payload.get("list") or []
        if not page_listings:
            break
        for raw in page_listings:
            parsed = parse_listing(raw, exchange_rates, currency_map)
            if parsed:
                listings.append(parsed)
        page += 1
        time.sleep(0.15)
    return listings


def fetch_exchange_rates() -> dict[str, float]:
    xml_text = fetch_text(ECB_URL)
    if not xml_text:
        return {"USD": 1.0}
    root = ET.fromstring(xml_text)
    cube_entries = root.findall(".//{*}Cube[@currency][@rate]")
    ecb_rates = {entry.attrib["currency"].upper(): float(entry.attrib["rate"]) for entry in cube_entries}
    usd_per_eur = ecb_rates.get("USD", 1.0)
    usd_rates = {"USD": 1.0}
    for code, per_eur in ecb_rates.items():
        usd_rates[code] = per_eur / usd_per_eur
    return usd_rates


def fetch_bricklink_currency_map() -> dict[int, str]:
    text = fetch_text(BRICKLINK_CURRENCY_VARS_URL)
    result: dict[int, str] = {}
    if not text:
        return result
    for match in BRICKLINK_CURRENCY_PATTERN.finditer(text):
        result[int(match.group(1))] = match.group(2).upper()
    return result


def fetch_lego_snapshot(official_url: str) -> dict[str, Any] | None:
    safe_url = official_url.strip()
    if not safe_url:
        return None
    wrapped = R_JINA_PREFIX + safe_url
    markdown = fetch_text(wrapped)
    if not markdown:
        return None
    price_match = MONEY_PATTERN.search(markdown)
    availability = None
    for label in ("Coming Soon", "Pre-order", "Preorder", "Not yet available", "Available now", "Sold out", "Backorder"):
        if label.lower() in markdown.lower():
            availability = label
            break
    return {
        "priceUsd": parse_money(price_match.group(1)) if price_match else None,
        "availability": availability,
    }


def build_official_price_condition(price_usd: float) -> dict[str, Any]:
    return {
        "estimateUsd": round_currency(price_usd),
        "minUsd": round_currency(price_usd),
        "maxUsd": round_currency(price_usd),
        "confidence": "High",
        "methodology": "Official LEGO Store price cached in the daily static pricing snapshot.",
        "sourceCoverage": ["LEGO Store"],
        "directSource": "LEGO Store official price",
        "freshnessLabel": "Daily static snapshot",
        "evidenceLabel": "Daily LEGO Store snapshot",
        "compCount": 1,
        "soldSourceCount": 1,
        "spreadRatio": 0.0,
        "freshestAgeDays": 0
    }


def build_set_condition(prices: list[float]) -> dict[str, Any] | None:
    if not prices:
        return None
    average = round_currency(sum(prices) / len(prices))
    low = round_currency(min(prices))
    high = round_currency(max(prices))
    spread_ratio = round_currency((high - low) / average) if average > 0 else None
    confidence = grade_set_confidence(len(prices), spread_ratio)
    return {
        "estimateUsd": average,
        "minUsd": low,
        "maxUsd": high,
        "confidence": confidence,
        "methodology": "Current value from the simple arithmetic average of BrickLink current listing prices in the matching condition bucket only.",
        "sourceCoverage": ["BrickLink"],
        "directSource": "BrickLink current listings average",
        "freshnessLabel": "Daily static snapshot",
        "evidenceLabel": set_evidence_label(len(prices), confidence),
        "compCount": len(prices),
        "soldSourceCount": 1,
        "spreadRatio": spread_ratio,
        "freshestAgeDays": 0,
    }


def build_minifigure_condition(prices: list[float]) -> dict[str, Any] | None:
    if not prices:
        return None
    average = round_currency(sum(prices) / len(prices))
    low = round_currency(min(prices))
    high = round_currency(max(prices))
    spread_ratio = round_currency((high - low) / average) if average > 0 else None
    confidence = grade_minifigure_confidence(len(prices), spread_ratio)
    return {
        "estimateUsd": average,
        "minUsd": low,
        "maxUsd": high,
        "confidence": confidence,
        "methodology": "Current minifigure value derived from BrickLink listing averages in the matching condition bucket only.",
        "sourceCoverage": ["BrickLink"],
        "directSource": "BrickLink current listings average",
        "freshnessLabel": "Daily static snapshot",
        "evidenceLabel": minifigure_evidence_label(len(prices), confidence),
        "compCount": len(prices),
        "soldSourceCount": 1,
        "spreadRatio": spread_ratio,
        "freshestAgeDays": 0,
    }


def filter_set_bucket(listings: list[dict[str, Any]], code_new: str, code_complete: str) -> list[float]:
    return [listing["priceUsd"] for listing in listings if listing["codeNew"] == code_new and listing["codeComplete"] == code_complete]


def filter_minifigure_bucket(listings: list[dict[str, Any]], code_new: str) -> list[float]:
    return [listing["priceUsd"] for listing in listings if listing["codeNew"] == code_new]


def parse_listing(raw: dict[str, Any], exchange_rates: dict[str, float], currency_map: dict[int, str]) -> dict[str, Any] | None:
    price_text = raw.get("mInvSalePrice") or raw.get("mDisplaySalePrice")
    if not price_text:
        return None
    amount = parse_money_amount(str(price_text))
    if amount is None:
        return None
    currency_id = raw.get("idCurrencyStore")
    try:
        currency_id = int(currency_id) if currency_id is not None else None
    except (TypeError, ValueError):
        currency_id = None
    currency_code = currency_map.get(currency_id) if currency_id is not None else detect_currency_code(str(price_text))
    if not currency_code:
        return None
    rate = exchange_rates.get(currency_code, 1.0)
    price_usd = amount if currency_code == "USD" else amount / rate
    if price_usd <= 0:
        return None
    code_new = str(raw.get("codeNew") or "").strip().upper()
    code_complete = str(raw.get("codeComplete") or "").strip().upper()
    if not code_new:
        return None
    return {
        "priceUsd": round_currency(price_usd),
        "quantity": int(raw.get("n4Qty") or 1),
        "codeNew": code_new,
        "codeComplete": code_complete,
    }


def build_listing_url(item_id: str, page: int, page_size: int) -> str:
    return (
        f"{BRICKLINK_LISTING_AJAX_URL}?itemid={item_id}&color=-1&st=1&ss=&cond=A&min=&max=&minqty=&nmp=0"
        f"&nosuperlot=1&ii=1&ic=1&is=1&loc=&reg=&ca=0&pmt=&rpp={page_size}&pi={page}&iconly=0"
    )


def fetch_json(url: str, referer: str | None = None) -> dict[str, Any] | None:
    text = fetch_text(url, accept="application/json", referer=referer)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def fetch_text(
    url: str,
    accept: str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    referer: str | None = None,
) -> str | None:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest" if accept == "application/json" else "",
            "Referer": referer or "",
        },
    )
    try:
        with OPENER.open(request, timeout=20) as response:
            return response.read().decode("utf-8", errors="replace")
    except Exception:
        return None


def parse_money(raw: str) -> float | None:
    return parse_money_amount(raw)


def parse_money_amount(raw: str) -> float | None:
    cleaned = re.sub(r"[^0-9.]", "", raw.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def detect_currency_code(raw: str) -> str | None:
    normalized = raw.strip().upper()
    if normalized.startswith(("US $", "USD", "$")):
        return "USD"
    if normalized.startswith(("EUR", "€")):
        return "EUR"
    if normalized.startswith(("GBP", "£")):
        return "GBP"
    if normalized.startswith("CHF"):
        return "CHF"
    if normalized.startswith(("CA $", "CAD")):
        return "CAD"
    if normalized.startswith(("AU $", "AUD")):
        return "AUD"
    return None


def grade_set_confidence(listing_count: int, spread_ratio: float | None) -> str:
    if listing_count >= 12 and (spread_ratio or 0.0) <= 0.55:
        return "High"
    if listing_count >= 5:
        return "Medium"
    if listing_count >= 1:
        return "Low"
    return "Fallback"


def grade_minifigure_confidence(listing_count: int, spread_ratio: float | None) -> str:
    if listing_count >= 10 and (spread_ratio or 0.0) <= 0.60:
        return "High"
    if listing_count >= 4:
        return "Medium"
    if listing_count >= 1:
        return "Low"
    return "Fallback"


def set_evidence_label(listing_count: int, confidence: str) -> str:
    if confidence == "High":
        return f"{listing_count} live BrickLink listings"
    if confidence == "Medium":
        return f"{listing_count} BrickLink listings"
    return "Thin BrickLink listing data"


def minifigure_evidence_label(listing_count: int, confidence: str) -> str:
    if confidence == "High":
        return f"{listing_count} live BrickLink listings"
    if confidence == "Medium":
        return f"{listing_count} BrickLink listings"
    return "Thin BrickLink listing data"


def round_currency(value: float) -> float:
    return round(value + 1e-9, 2)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("™", "").replace("®", "")).strip()


def slugify_name(value: str) -> str:
    cleaned = clean_text(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    return cleaned.strip("-")


def similarity_score(left: str, right: str) -> float:
    left_tokens = set(re.findall(r"[a-z0-9]+", left.lower()))
    right_tokens = set(re.findall(r"[a-z0-9]+", right.lower()))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    exact_bonus = 0.2 if clean_text(left).lower() == clean_text(right).lower() else 0.0
    prefix_bonus = 0.1 if clean_text(right).lower().startswith(clean_text(left).lower().split(",")[0]) else 0.0
    return (overlap / union) + exact_bonus + prefix_bonus


if __name__ == "__main__":
    main()
