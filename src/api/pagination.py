from urllib.parse import urlencode
from fastapi import Request

def build_page_url(request: Request, new_offset: int) -> str:
    params = dict(request.query_params)
    params["offset"] = str(new_offset)
    return f"{str(request.base_url).rstrip('/')}{request.url.path}?{urlencode(params)}"

def build_pagination_links(
    request: Request,
    offset: int,
    limit: int,
    total: int,
) -> tuple[str | None, str | None]:
    next_url = build_page_url(request, offset + limit) if offset + limit < total else None
    prev_url = build_page_url(request, max(0, offset - limit)) if offset > 0 else None
    return next_url, prev_url