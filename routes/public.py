from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging
import uuid

from database import get_db, async_session_maker
from models import QRCode, QRScan
from utils import parse_device_info, get_location_from_ip
from utils_session import is_new_user_atomic
from config import settings

router = APIRouter(tags=["Public"])
logger = logging.getLogger(__name__)


async def _enrich_location(scan_id: int, ip_address: str):
    """
    Runs after the response is already sent.
    Looks up city/country from IP and updates the scan row.
    Uses its own DB session — never touches the request session.
    """
    async with async_session_maker() as db:
        try:
            location_data = await get_location_from_ip(ip_address)
            if not location_data:
                return

            result = await db.execute(select(QRScan).where(QRScan.id == scan_id))
            scan = result.scalar_one_or_none()
            if scan:
                scan.country = location_data.get("country")
                scan.city    = location_data.get("city")
                scan.region  = location_data.get("region")
                await db.commit()
                logger.info(f"📍 Location enriched for scan #{scan_id}: {scan.city}, {scan.country}")
        except Exception as e:
            logger.error(f"Location enrich failed for scan #{scan_id}: {e}")


@router.get("/r/{code}")
async def redirect_qr(code: str, request: Request, db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(
            select(QRCode.id, QRCode.target_url, QRCode.is_active, QRCode.code)
            .where(QRCode.code == code)
        )
        qr_data = result.one_or_none()

        if not qr_data:
            raise HTTPException(status_code=404, detail="QR code not found")

        qr_id, target_url, is_active, qr_code = qr_data

        if not is_active:
            raise HTTPException(status_code=410, detail="QR code deactivated")

        separator = "&" if "?" in target_url else "?"
        redirect_url = f"{target_url}{separator}branch={qr_code}"

        session_id = request.cookies.get("qr_session") or str(uuid.uuid4())

        html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Redirecting...</title>
</head>
<body>
<script>
const payload = {{
    qr_code_id: {qr_id},
    user_agent: navigator.userAgent,
    session_id: "{session_id}"
}};

const sent = navigator.sendBeacon(
    "{settings.BASE_URL}/api/scan-log",
    new Blob([JSON.stringify(payload)], {{ type: 'application/json' }})
);

if (!sent) {{
    fetch("{settings.BASE_URL}/api/scan-log", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify(payload),
        keepalive: true
    }}).catch(() => {{}});
}}

setTimeout(() => {{ window.location.replace("{redirect_url}"); }}, 100);
</script>
</body>
</html>"""

        response = HTMLResponse(content=html_content)
        response.set_cookie(
            key="qr_session",
            value=session_id,
            max_age=60 * 60 * 24 * 365,
            httponly=False,
            samesite="None",
            secure=True,
            path="/"
        )
        return response

    except Exception as e:
        logger.error(f"Redirect error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal error")


@router.post("/api/scan-log")
async def log_scan(
    request: Request,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    1. Parse device info (instant, no I/O)
    2. Atomic new-vs-returning check (single DB insert, ~5ms)
    3. Save scan with ip_address but country=None  (~5ms)
    4. Return 200 immediately  ← user's browser redirects now
    5. IP lookup happens in background, updates the row silently
    """
    try:
        data = await request.json()

        qr_code_id       = data.get("qr_code_id")
        user_agent       = data.get("user_agent", "")
        frontend_session = data.get("session_id", "")

        ip_address     = request.client.host if request.client else None
        cookie_session = request.cookies.get("qr_session")

        session_id = frontend_session or cookie_session or str(uuid.uuid4())
        if not frontend_session and not cookie_session:
            logger.warning(f"No session for QR {qr_code_id}, created fallback")

        device_info = parse_device_info(user_agent)

        is_new = await is_new_user_atomic(
            db,
            session_id,
            action_type="qr_scan",
            qr_code_id=qr_code_id
        )

        # Save immediately — no waiting for external API
        scan = QRScan(
            qr_code_id  = qr_code_id,
            device_type = device_info["device_type"],
            device_name = device_info["device_name"],
            browser     = device_info["browser"],
            os          = device_info["os"],
            ip_address  = ip_address,
            country     = None,   # filled in by background task
            city        = None,
            region      = None,
            session_id  = session_id,
            is_new_user = is_new,
            user_agent  = user_agent
        )

        db.add(scan)
        await db.commit()
        await db.refresh(scan)

        # Enrich location after response is sent — never blocks the user
        if ip_address:
            background_tasks.add_task(_enrich_location, scan.id, ip_address)

        logger.info(f"Scan #{scan.id} | QR={qr_code_id} session={session_id[:8]}... new={is_new}")
        return {"status": "success", "scan_id": scan.id, "is_new_user": is_new}

    except Exception as e:
        logger.error(f"Scan log error: {e}", exc_info=True)
        await db.rollback()
        return {"status": "error"}