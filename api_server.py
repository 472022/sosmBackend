from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, Response

try:
    from fastapi.middleware.cors import CORSMiddleware
except Exception:
    CORSMiddleware = None


def _parse_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    s = v.strip().lower()
    if s in {"1", "true", "yes"}:
        return True
    if s in {"0", "false", "no"}:
        return False
    return None


def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.strip().lower()


def _contains(haystack: Optional[str], needle: str) -> bool:
    if not needle:
        return True
    if not haystack:
        return False
    return needle in haystack.strip().lower()


def _clamp_int(value: int, min_v: int, max_v: int) -> int:
    if value < min_v:
        return min_v
    if value > max_v:
        return max_v
    return value


def _paginate(items: list[Any], offset: int, limit: int) -> list[Any]:
    if offset < 0:
        offset = 0
    if limit < 0:
        limit = 0
    return items[offset : offset + limit]


def _etag_for_paths(paths: list[Path]) -> str:
    parts: list[str] = []
    for p in paths:
        try:
            st = p.stat()
            parts.append(f"{p.name}:{int(st.st_mtime)}:{st.st_size}")
        except FileNotFoundError:
            parts.append(f"{p.name}:missing")
    raw = "|".join(parts).encode("utf-8")
    return '"' + hashlib.sha1(raw).hexdigest() + '"'


def _set_etag_or_304(request: Request, response: Response, etag: str) -> Optional[Response]:
    response.headers["ETag"] = etag
    inm = request.headers.get("if-none-match")
    if inm is not None and inm == etag:
        return Response(status_code=304, headers={"ETag": etag})
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"Missing dataset file: {path.name}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in dataset file: {path.name}")


DATA_DIR = Path(os.environ.get("SOSM_DATA_DIR", Path(__file__).resolve().parent))

FILES: dict[str, Path] = {
    "stocks": DATA_DIR / "stock_data.json",
    "trending": DATA_DIR / "trending_data.json",
    "ipos": DATA_DIR / "ipo_data.json",
    "news": DATA_DIR / "news_data.json",
    "mutualFunds": DATA_DIR / "mutual_funds_data.json",
    "priceShockers": DATA_DIR / "price_shockers_data.json",
    "bseMostActive": DATA_DIR / "bse_most_active_data.json",
    "nseMostActive": DATA_DIR / "nse_most_active_data.json",
    "week52": DATA_DIR / "52_week_high_low_data.json",
}

app = FastAPI(title="SOSM JSON API", version="1.0.0")

origins_raw = os.environ.get("SOSM_ALLOWED_ORIGINS", "").strip()
if origins_raw and CORSMiddleware is not None:
    origins = [o.strip() for o in origins_raw.split(",") if o.strip()]
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET"],
            allow_headers=["*"],
        )


@app.get("/api/v1/health")
def health(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths(list(FILES.values()))
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm

    datasets: list[dict[str, Any]] = []
    for name, path in FILES.items():
        exists = path.exists()
        last_updated = None
        error = None
        if exists:
            decoded = _read_json(path)
            lu = decoded.get("last_updated")
            if isinstance(lu, str):
                last_updated = lu
            err = decoded.get("error")
            if isinstance(err, str):
                error = err
        datasets.append(
            {
                "name": name,
                "file": path.name,
                "exists": exists,
                "last_updated": last_updated,
                "error": error,
            }
        )
    return {"ok": True, "data": {"datasets": datasets}, "error": None}


@app.get("/api/v1/sources")
def sources(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths(list(FILES.values()))
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    return {
        "ok": True,
        "data": {"sources": [{"name": k, "file": v.name} for k, v in FILES.items()]},
        "error": None,
    }


@app.get("/api/v1/stocks")
def list_stocks(
    request: Request,
    response: Response,
    symbols: Optional[str] = None,
    stale: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["stocks"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm

    decoded = _read_json(FILES["stocks"])
    stocks_map = decoded.get("stocks")
    if not isinstance(stocks_map, dict):
        raise HTTPException(status_code=500, detail="Invalid stocks dataset structure")

    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    errors_map = decoded.get("errors") if isinstance(decoded.get("errors"), dict) else {}

    items: list[dict[str, Any]] = []
    for sym, rec in stocks_map.items():
        if not isinstance(rec, dict):
            continue
        items.append(
            {
                "symbol": rec.get("symbol") if isinstance(rec.get("symbol"), str) else str(sym),
                "name": rec.get("name") if isinstance(rec.get("name"), str) else None,
                "cached": bool(rec.get("cached", False)),
                "stale": bool(rec.get("stale", False)),
                "latency_ms": rec.get("latency_ms"),
                "error": errors_map.get(str(sym)) if isinstance(errors_map.get(str(sym)), str) else None,
            }
        )

    items.sort(key=lambda x: str(x.get("symbol", "")))

    if symbols:
        want = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        items = [it for it in items if str(it.get("symbol", "")).upper() in want]

    stale_bool = _parse_bool(stale)
    if stale_bool is not None:
        items = [it for it in items if bool(it.get("stale")) is stale_bool]

    qn = _norm(q)
    if qn:
        items = [it for it in items if _contains(str(it.get("symbol", "")), qn) or _contains(str(it.get("name", "")), qn)]

    limit = _clamp_int(limit, 0, 1000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    return {"ok": True, "last_updated": last_updated, "data": items, "error": None, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/stocks/{symbol}")
def get_stock(request: Request, response: Response, symbol: str) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["stocks"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm

    decoded = _read_json(FILES["stocks"])
    stocks_map = decoded.get("stocks")
    if not isinstance(stocks_map, dict):
        raise HTTPException(status_code=500, detail="Invalid stocks dataset structure")

    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    errors_map = decoded.get("errors") if isinstance(decoded.get("errors"), dict) else {}

    key = symbol.upper()
    rec = stocks_map.get(key) or stocks_map.get(symbol)
    if not isinstance(rec, dict):
        raise HTTPException(status_code=404, detail="Stock symbol not found")

    err = errors_map.get(key) if isinstance(errors_map.get(key), str) else errors_map.get(symbol)
    out = dict(rec)
    if isinstance(err, str):
        out["error"] = err
    else:
        out["error"] = None
    return {"ok": True, "last_updated": last_updated, "data": out, "error": None}


@app.get("/api/v1/stocks/{symbol}/raw")
def get_stock_raw(request: Request, response: Response, symbol: str) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["stocks"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm

    decoded = _read_json(FILES["stocks"])
    stocks_map = decoded.get("stocks")
    if not isinstance(stocks_map, dict):
        raise HTTPException(status_code=500, detail="Invalid stocks dataset structure")

    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    key = symbol.upper()
    rec = stocks_map.get(key) or stocks_map.get(symbol)
    if not isinstance(rec, dict):
        raise HTTPException(status_code=404, detail="Stock symbol not found")
    return {"ok": True, "last_updated": last_updated, "data": rec.get("data"), "error": None}


@app.get("/api/v1/market/trending")
def trending_all(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["trending"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["trending"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    ts = root.get("trending_stocks") if isinstance(root.get("trending_stocks"), dict) else {}
    gainers = ts.get("top_gainers") if isinstance(ts.get("top_gainers"), list) else []
    losers = ts.get("top_losers") if isinstance(ts.get("top_losers"), list) else []
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": {"top_gainers": gainers, "top_losers": losers}, "error": err}


def _trending_bucket(
    bucket: str,
    request: Request,
    response: Response,
    q: Optional[str],
    exchange_type: Optional[str],
    limit: int,
    offset: int,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["trending"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["trending"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    ts = root.get("trending_stocks") if isinstance(root.get("trending_stocks"), dict) else {}
    items = ts.get(bucket) if isinstance(ts.get(bucket), list) else []

    qn = _norm(q)
    if qn:
        items = [it for it in items if isinstance(it, dict) and (_contains(it.get("company_name"), qn) or _contains(it.get("ric"), qn))]

    ex = _norm(exchange_type)
    if ex:
        items = [it for it in items if isinstance(it, dict) and _norm(str(it.get("exchange_type") or "")) == ex]

    limit = _clamp_int(limit, 0, 1000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": items, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/market/trending/gainers")
def trending_gainers(
    request: Request,
    response: Response,
    q: Optional[str] = None,
    exchange_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    return _trending_bucket("top_gainers", request, response, q, exchange_type, limit, offset)


@app.get("/api/v1/market/trending/losers")
def trending_losers(
    request: Request,
    response: Response,
    q: Optional[str] = None,
    exchange_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    return _trending_bucket("top_losers", request, response, q, exchange_type, limit, offset)


def _date_in_range(d: Optional[str], date_from: Optional[str], date_to: Optional[str]) -> bool:
    if d is None:
        return True
    if date_from and d < date_from:
        return False
    if date_to and d > date_to:
        return False
    return True


@app.get("/api/v1/market/ipos")
def list_ipos(
    request: Request,
    response: Response,
    status: Optional[str] = None,
    is_sme: Optional[str] = None,
    q: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["ipos"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["ipos"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    upcoming = root.get("upcoming") if isinstance(root.get("upcoming"), list) else []
    listed = root.get("listed") if isinstance(root.get("listed"), list) else []
    closed = root.get("closed") if isinstance(root.get("closed"), list) else []

    st = _norm(status)
    if not st:
        items: list[Any] = [*upcoming, *listed, *closed]
    elif st == "upcoming":
        items = list(upcoming)
    elif st == "listed":
        items = list(listed)
    elif st == "closed":
        items = list(closed)
    else:
        raise HTTPException(status_code=400, detail="Invalid status")

    sme_bool = _parse_bool(is_sme)
    if sme_bool is not None:
        items = [it for it in items if isinstance(it, dict) and bool(it.get("is_sme", False)) is sme_bool]

    qn = _norm(q)
    if qn:
        items = [it for it in items if isinstance(it, dict) and (_contains(it.get("name"), qn) or _contains(it.get("symbol"), qn))]

    if date_from or date_to:
        def _pick_date(it: dict[str, Any]) -> Optional[str]:
            d = it.get("bidding_start_date") or it.get("listing_date")
            return d if isinstance(d, str) else None

        items = [it for it in items if isinstance(it, dict) and _date_in_range(_pick_date(it), date_from, date_to)]

    limit = _clamp_int(limit, 0, 1000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": items, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/market/ipos/{symbol}")
def get_ipo(request: Request, response: Response, symbol: str) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["ipos"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["ipos"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    all_items: list[Any] = []
    for key in ("upcoming", "listed", "closed"):
        v = root.get(key)
        if isinstance(v, list):
            all_items.extend(v)
    target = symbol.upper()
    for it in all_items:
        if isinstance(it, dict) and isinstance(it.get("symbol"), str) and it["symbol"].upper() == target:
            err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
            return {"ok": True, "last_updated": last_updated, "data": it, "error": err}
    raise HTTPException(status_code=404, detail="IPO symbol not found")


@app.get("/api/v1/market/news")
def list_news(
    request: Request,
    response: Response,
    source: Optional[str] = None,
    topic: Optional[str] = None,
    q: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["news"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["news"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    items = decoded.get("data") if isinstance(decoded.get("data"), list) else []

    qn = _norm(q)
    if qn:
        items = [it for it in items if isinstance(it, dict) and (_contains(it.get("title"), qn) or _contains(it.get("summary"), qn))]

    if source:
        items = [it for it in items if isinstance(it, dict) and it.get("source") == source]

    if topic:
        items = [it for it in items if isinstance(it, dict) and isinstance(it.get("topics"), list) and topic in it["topics"]]

    if date_from or date_to:
        def _pub_date(it: dict[str, Any]) -> Optional[str]:
            pd = it.get("pub_date")
            if not isinstance(pd, str) or len(pd) < 10:
                return None
            return pd[:10]

        items = [it for it in items if isinstance(it, dict) and _date_in_range(_pub_date(it), date_from, date_to)]

    limit = _clamp_int(limit, 0, 500)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": items, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/market/news/sources")
def news_sources(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["news"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["news"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    items = decoded.get("data") if isinstance(decoded.get("data"), list) else []

    counts: dict[str, int] = {}
    for it in items:
        if isinstance(it, dict) and isinstance(it.get("source"), str) and it["source"].strip():
            counts[it["source"]] = counts.get(it["source"], 0) + 1
    out = [{"source": k, "count": counts[k]} for k in sorted(counts)]
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": out, "error": err}


@app.get("/api/v1/market/news/topics")
def news_topics(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["news"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["news"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    items = decoded.get("data") if isinstance(decoded.get("data"), list) else []

    counts: dict[str, int] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        topics = it.get("topics")
        if not isinstance(topics, list):
            continue
        for t in topics:
            if isinstance(t, str) and t.strip():
                counts[t] = counts.get(t, 0) + 1
    out = [{"topic": k, "count": counts[k]} for k in sorted(counts)]
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": out, "error": err}


@app.get("/api/v1/market/mutual-funds")
def mutual_fund_taxonomy(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["mutualFunds"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["mutualFunds"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}

    taxonomy: list[dict[str, Any]] = []
    for cat, subcats in root.items():
        if isinstance(subcats, dict):
            taxonomy.append({"category": str(cat), "subcategories": [str(k) for k in subcats.keys()]})

    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": taxonomy, "error": err}


def _flatten_mutual_funds(decoded: dict[str, Any]) -> list[dict[str, Any]]:
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    out: list[dict[str, Any]] = []
    for cat, subcats in root.items():
        if not isinstance(subcats, dict):
            continue
        for subcat, funds in subcats.items():
            if not isinstance(funds, list):
                continue
            for f in funds:
                if not isinstance(f, dict):
                    continue
                rec = dict(f)
                rec["category"] = str(cat)
                rec["subcategory"] = str(subcat)
                out.append(rec)
    return out


@app.get("/api/v1/market/mutual-funds/funds")
def mutual_funds(
    request: Request,
    response: Response,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    q: Optional[str] = None,
    min_star_rating: int = -1,
    min_return_1y: Optional[float] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["mutualFunds"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["mutualFunds"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    flat = _flatten_mutual_funds(decoded)

    if category:
        flat = [f for f in flat if f.get("category") == category]
    if subcategory:
        flat = [f for f in flat if f.get("subcategory") == subcategory]

    qn = _norm(q)
    if qn:
        flat = [f for f in flat if _contains(str(f.get("fund_name") or ""), qn)]

    if min_star_rating >= 0:
        min_star_rating = _clamp_int(min_star_rating, 0, 5)
        def _sr_ok(f: dict[str, Any]) -> bool:
            sr = f.get("star_rating")
            if sr is None:
                return False
            try:
                return int(sr) >= min_star_rating
            except Exception:
                return False

        flat = [f for f in flat if _sr_ok(f)]

    if min_return_1y is not None:
        def _r_ok(f: dict[str, Any]) -> bool:
            r = f.get("1_year_return")
            try:
                return r is not None and float(r) >= float(min_return_1y)
            except Exception:
                return False

        flat = [f for f in flat if _r_ok(f)]

    limit = _clamp_int(limit, 0, 2000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(flat)
    flat = _paginate(flat, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": flat, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/market/mutual-funds/funds/{fund_name}")
def mutual_fund_by_name(request: Request, response: Response, fund_name: str) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["mutualFunds"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["mutualFunds"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    flat = _flatten_mutual_funds(decoded)
    needle = _norm(fund_name)
    for f in flat:
        if _norm(str(f.get("fund_name") or "")) == needle:
            err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
            return {"ok": True, "last_updated": last_updated, "data": f, "error": err}
    raise HTTPException(status_code=404, detail="Fund not found")


@app.get("/api/v1/market/price-shockers")
def price_shockers_all(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["priceShockers"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["priceShockers"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    bse = root.get("BSE_PriceShocker") if isinstance(root.get("BSE_PriceShocker"), list) else []
    nse = root.get("NSE_PriceShocker") if isinstance(root.get("NSE_PriceShocker"), list) else []
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": {"bse": bse, "nse": nse}, "error": err}


@app.get("/api/v1/market/price-shockers/{exchange}")
def price_shockers_exchange(
    request: Request,
    response: Response,
    exchange: str,
    q: Optional[str] = None,
    direction: Optional[str] = None,
    min_deviation: Optional[float] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["priceShockers"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["priceShockers"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}

    ex = exchange.lower()
    if ex == "bse":
        items = root.get("BSE_PriceShocker") if isinstance(root.get("BSE_PriceShocker"), list) else []
    elif ex == "nse":
        items = root.get("NSE_PriceShocker") if isinstance(root.get("NSE_PriceShocker"), list) else []
    else:
        raise HTTPException(status_code=404, detail="Route not found")

    qn = _norm(q)
    if qn:
        items = [
            it
            for it in items
            if isinstance(it, dict)
            and (
                _contains(it.get("displayName"), qn)
                or _contains(it.get("ric"), qn)
                or _contains(it.get("nseCode"), qn)
            )
        ]

    dirn = _norm(direction)
    if dirn:
        def _dir_ok(it: dict[str, Any]) -> bool:
            ad = it.get("actualDeviation")
            try:
                if ad is not None:
                    adf = float(ad)
                    if dirn == "up":
                        return adf > 0
                    if dirn == "down":
                        return adf < 0
            except Exception:
                pass
            arrow = it.get("priceArrow")
            if dirn == "up":
                return arrow == "⇧"
            if dirn == "down":
                return arrow == "⇩"
            return True

        items = [it for it in items if isinstance(it, dict) and _dir_ok(it)]

    if min_deviation is not None:
        def _dev_ok(it: dict[str, Any]) -> bool:
            v = it.get("actualDeviation") if it.get("actualDeviation") is not None else it.get("deviation")
            try:
                return v is not None and abs(float(v)) >= float(min_deviation)
            except Exception:
                return False

        items = [it for it in items if isinstance(it, dict) and _dev_ok(it)]

    limit = _clamp_int(limit, 0, 1000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": items, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/market/most-active")
def most_active_all(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["bseMostActive"], FILES["nseMostActive"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    bse = _read_json(FILES["bseMostActive"])
    nse = _read_json(FILES["nseMostActive"])
    return {
        "ok": True,
        "data": {
            "bse": {"last_updated": bse.get("last_updated"), "data": bse.get("data"), "error": bse.get("error")},
            "nse": {"last_updated": nse.get("last_updated"), "data": nse.get("data"), "error": nse.get("error")},
        },
        "error": None,
    }


@app.get("/api/v1/market/most-active/{exchange}")
def most_active_exchange(
    request: Request,
    response: Response,
    exchange: str,
    q: Optional[str] = None,
    min_volume: int = -1,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["bseMostActive"], FILES["nseMostActive"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm

    ex = exchange.lower()
    if ex == "bse":
        decoded = _read_json(FILES["bseMostActive"])
    elif ex == "nse":
        decoded = _read_json(FILES["nseMostActive"])
    else:
        raise HTTPException(status_code=404, detail="Route not found")

    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    items = decoded.get("data") if isinstance(decoded.get("data"), list) else []

    qn = _norm(q)
    if qn:
        items = [it for it in items if isinstance(it, dict) and (_contains(it.get("ticker"), qn) or _contains(it.get("company"), qn))]

    if min_volume >= 0:
        items = [it for it in items if isinstance(it, dict) and isinstance(it.get("volume"), (int, float)) and int(it["volume"]) >= min_volume]

    limit = _clamp_int(limit, 0, 1000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": items, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/market/52-week")
def week52_all(request: Request, response: Response) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["week52"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm
    decoded = _read_json(FILES["week52"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": root, "error": err}


@app.get("/api/v1/market/52-week/{exchange}/{side}")
def week52_exchange(
    request: Request,
    response: Response,
    exchange: str,
    side: str,
    q: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    etag = _etag_for_paths([FILES["week52"]])
    nm = _set_etag_or_304(request, response, etag)
    if nm is not None:
        return nm

    decoded = _read_json(FILES["week52"])
    last_updated = decoded.get("last_updated") if isinstance(decoded.get("last_updated"), str) else None
    root = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}

    ex = exchange.lower()
    if ex == "bse":
        ex_key = "BSE_52WeekHighLow"
    elif ex == "nse":
        ex_key = "NSE_52WeekHighLow"
    else:
        raise HTTPException(status_code=404, detail="Route not found")

    sd = side.lower()
    if sd == "high":
        bucket = "high52Week"
    elif sd == "low":
        bucket = "low52Week"
    else:
        raise HTTPException(status_code=404, detail="Route not found")

    ex_obj = root.get(ex_key) if isinstance(root.get(ex_key), dict) else {}
    items = ex_obj.get(bucket) if isinstance(ex_obj.get(bucket), list) else []

    qn = _norm(q)
    if qn:
        items = [it for it in items if isinstance(it, dict) and (_contains(it.get("ticker"), qn) or _contains(it.get("company"), qn))]

    limit = _clamp_int(limit, 0, 1000)
    offset = _clamp_int(offset, 0, 1_000_000)
    total = len(items)
    items = _paginate(items, offset, limit)
    err = decoded.get("error") if isinstance(decoded.get("error"), str) else None
    return {"ok": True, "last_updated": last_updated, "data": items, "error": err, "meta": {"total": total, "limit": limit, "offset": offset}}


@app.get("/api/v1/info")
def info() -> dict[str, Any]:
    return {
        "ok": True,
        "data": {
            "service_time_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data_dir": str(DATA_DIR),
            "datasets": [{"name": k, "file": v.name} for k, v in FILES.items()],
        },
        "error": None,
    }
