from fastapi import APIRouter, BackgroundTasks, Request, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from pathlib import Path
from typing import Optional
import logging
import uuid

from database import get_db, async_session_maker
from models import SocialClick, QRCode
from utils import parse_device_info, get_location_from_ip
from utils_session import is_new_user_atomic

router = APIRouter(tags=["Social Links"])
logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path("templates/social")


async def _enrich_click_location(click_id: int, ip_address: str):
    """Updates city/country on an existing SocialClick after the response is sent."""
    async with async_session_maker() as db:
        try:
            location_data = await get_location_from_ip(ip_address)
            if not location_data:
                return

            result = await db.execute(select(SocialClick).where(SocialClick.id == click_id))
            click = result.scalar_one_or_none()
            if click:
                click.country = location_data.get("country")
                click.city    = location_data.get("city")
                await db.commit()
        except Exception as e:
            logger.error(f"Location enrich failed for click #{click_id}: {e}")


@router.get("/social-links", response_class=HTMLResponse)
async def social_links_page(
    request: Request,
    branch: Optional[str] = Query(None),
):
    try:
        html_path = TEMPLATES_DIR / "index.html"
        if not html_path.exists():
            return HTMLResponse("<h1>Social Links page not found</h1>", status_code=404)

        html_content = html_path.read_text(encoding="utf-8")

        if branch:
            html_content = html_content.replace(
                "const BRANCH_CODE = null;",
                f'const BRANCH_CODE = "{branch}";',
            )

        return HTMLResponse(content=html_content)

    except Exception as exc:
        logger.error(f"Error loading social links page: {exc}", exc_info=True)
        return HTMLResponse(f"<h1>Error: {exc}</h1>", status_code=500)


@router.post("/api/social-click")
async def log_social_click(
    request: Request,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    try:
        data = await request.json()

        platform      = data.get("platform", "unknown")
        branch_code   = data.get("branch_code")
        user_agent    = request.headers.get("user-agent", "")
        ip_address    = request.client.host if request.client else None

        cookie_session = request.cookies.get("qr_session")
        session_id = cookie_session or data.get("session_id", "") or str(uuid.uuid4())

        # Resolve branch_id
        branch_id = None
        if branch_code:
            row = await db.execute(
                select(QRCode.branch_id).where(QRCode.code == branch_code)
            )
            branch_id = row.scalar_one_or_none()

        device_info = parse_device_info(user_agent)

        is_new = await is_new_user_atomic(
            db,
            session_id,
            action_type="social_click",
            branch_id=branch_id
        )

        # Save immediately — no waiting for external API
        click = SocialClick(
            platform    = platform,
            branch_id   = branch_id,
            device_type = device_info["device_type"],
            browser     = device_info["browser"],
            os          = device_info["os"],
            ip_address  = ip_address,
            country     = None,   # filled in by background task
            city        = None,
            session_id  = session_id,
            is_new_user = is_new,
            user_agent  = user_agent,
        )

        db.add(click)
        await db.commit()
        await db.refresh(click)

        if ip_address:
            background_tasks.add_task(_enrich_click_location, click.id, ip_address)

        logger.info(f"Social click recorded: {platform} (Session: {session_id[:8]}...)")
        return {"status": "success", "is_new_user": is_new}

    except Exception as e:
        logger.error(f"Error logging social click: {e}", exc_info=True)
        return {"status": "error"}


@router.get("/api/social-analytics")
async def get_social_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    branch_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    try:
        from datetime import datetime, timedelta

        filters = []
        if branch_id:
            filters.append(SocialClick.branch_id == branch_id)
        if start_date:
            filters.append(SocialClick.clicked_at >= datetime.fromisoformat(start_date))
        if end_date:
            end_dt = datetime.fromisoformat(end_date) + timedelta(days=1) - timedelta(seconds=1)
            filters.append(SocialClick.clicked_at <= end_dt)

        result = await db.execute(
            select(SocialClick.platform, func.count(SocialClick.id).label('count'))
            .where(and_(*filters) if filters else True)
            .group_by(SocialClick.platform)
            .order_by(func.count(SocialClick.id).desc())
        )

        rows = result.all()
        return {
            "total_clicks": sum(r.count for r in rows),
            "platforms": [{"platform": r.platform, "count": r.count} for r in rows],
            "branch_id": branch_id,
        }

    except Exception as e:
        logger.error(f"Analytics error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": "Failed to get analytics"})


@router.get("/social-links/styles.css")
async def social_links_css():
    css_path = TEMPLATES_DIR / "styles.css"
    if not css_path.exists():
        return HTMLResponse("", status_code=404)
    return HTMLResponse(css_path.read_text(), media_type="text/css")


@router.get("/social-links/{image_name}")
async def social_links_images(image_name: str):
    allowed = ["gk.png", "facebook.png", "instagram.png", "youtube.png",
               "threads.png", "twitter.png", "whatsapp.png"]
    if image_name not in allowed:
        return HTMLResponse("Not found", status_code=404)
    path = TEMPLATES_DIR / image_name
    if not path.exists():
        return HTMLResponse("Not found", status_code=404)
    return FileResponse(path)