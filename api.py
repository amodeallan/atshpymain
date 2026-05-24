from fastapi import FastAPI, Request, Query
import asyncio
import time
import logging
from typing import Dict, Any, Tuple, Optional
from asho import get_variant_and_token

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

# --- Configuration ---
WORKERS = 15                # optimal workers
RATE_DELAY = 1.5           # delay antar request
RESULT_TIMEOUT = 180        # reasonable timeout
MAX_QUEUE = 5000            # reduced dari 20000 untuk stability
MAX_RETRIES = 2             # auto-retry
RETRY_DELAY = 0.5           # delay antar retry

# --- Queue & State Management ---
job_q: "asyncio.Queue[Tuple[Dict[str, Any], asyncio.Future]]" = asyncio.Queue(maxsize=MAX_QUEUE)
_last_dispatch = 0.0
_pace_lock = asyncio.Lock()
_active_jobs = set()

async def _pace(delay: float = RATE_DELAY) -> None:
    """Global pacing untuk avoid rate limiting"""
    global _last_dispatch
    async with _pace_lock:
        now = time.monotonic()
        wait = _last_dispatch + delay - now
        if wait > 0:
            await asyncio.sleep(wait)
        _last_dispatch = time.monotonic()

async def _worker(idx: int):
    """Worker dengan retry & error handling"""
    while True:
        body = None
        fut = None
        try:
            body, fut = await job_q.get()
            _active_jobs.add(id(fut))
            
            # Validasi input
            if not isinstance(body, dict) or "site" not in body or "cc" not in body:
                fut.set_result({"status": "error", "message": "Invalid request format"})
                continue
            
            site = body.get("site", "").strip()
            cc = body.get("cc", "").strip()
            
            if not site or not cc:
                fut.set_result({"status": "error", "message": "site and cc cannot be empty"})
                continue
            
            # Parse & validate CC
            try:
                parts = cc.split("|")
                if len(parts) != 4:
                    raise ValueError("CC format: number|mm|yy|cvv")
                cc_number, cc_month, cc_year, cc_cvv = parts
                
                if not (cc_number.isdigit() and len(cc_number) >= 13):
                    raise ValueError("Invalid card number")
                if not (cc_month.isdigit() and 1 <= int(cc_month) <= 12):
                    raise ValueError("Invalid month")
                if not (cc_year.isdigit() and len(cc_year) == 2):
                    raise ValueError("Invalid year")
                if not cc_cvv.isdigit():
                    raise ValueError("Invalid CVV")
            except ValueError as e:
                fut.set_result({"status": "error", "message": f"Invalid CC format: {str(e)}"})
                continue
            
            # Global pacing
            await _pace(RATE_DELAY)
            
            # Retry logic dengan exponential backoff
            last_error = None
            for attempt in range(MAX_RETRIES + 1):
                try:
                    result = await get_variant_and_token(
                        site, cc_number, cc_month, cc_year, cc_cvv
                    )
                    
                    # Validate result
                    if result is None:
                        raise Exception("Empty response from processor")
                    
                    if not isinstance(result, dict):
                        raise Exception(f"Invalid response type: {type(result)}")
                    
                    fut.set_result({"status": "success", "result": result})
                    logger.info(f"Worker-{idx}: Success on attempt {attempt + 1}")
                    break
                    
                except Exception as e:
                    last_error = str(e)
                    if attempt < MAX_RETRIES:
                        wait_time = RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"Worker-{idx}: Attempt {attempt + 1} failed: {last_error}, retrying in {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Worker-{idx}: All {MAX_RETRIES + 1} attempts failed: {last_error}")
            
            if not fut.done():
                fut.set_result({"status": "error", "message": f"Processing failed: {last_error}"})
                
        except Exception as e:
            logger.exception(f"Worker-{idx}: Unexpected error")
            if fut and not fut.done():
                fut.set_result({"status": "error", "message": f"worker-{idx}: {str(e)}"})
        finally:
            if fut:
                _active_jobs.discard(id(fut))
            if body is not None or fut is not None:
                job_q.task_done()

@app.on_event("startup")
async def _startup():
    """Start worker pool on app startup"""
    logger.info(f"Starting {WORKERS} workers...")
    for i in range(WORKERS):
        asyncio.create_task(_worker(i))
    logger.info("Workers started successfully")

@app.on_event("shutdown")
async def _shutdown():
    """Cleanup on app shutdown"""
    logger.info(f"Shutting down... Active jobs: {len(_active_jobs)}")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "OK",
        "message": "Shopify Gate API working",
        "active_jobs": len(_active_jobs),
        "queue_size": job_q.qsize()
    }

@app.get("/health")
async def health():
    """Detailed health check"""
    return {
        "status": "healthy",
        "workers": WORKERS,
        "queue_size": job_q.qsize(),
        "max_queue": MAX_QUEUE,
        "active_jobs": len(_active_jobs)
    }

def _validate_body(body: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate request body"""
    if not isinstance(body, dict):
        return False, "Invalid JSON body"
    
    for k in ("site", "cc"):
        if k not in body:
            return False, f"Missing required parameter: {k}"
    
    site = body.get("site", "").strip()
    cc = body.get("cc", "").strip()
    
    if not site:
        return False, "site cannot be empty"
    if not cc:
        return False, "cc cannot be empty"
    
    if not (site.startswith("http://") or site.startswith("https://")):
        return False, "site must be a valid URL"
    
    return True, ""

@app.post("/check")
async def check_card_post(data: Request):
    """POST endpoint for card checking"""
    try:
        body = await data.json()
    except Exception as e:
        logger.warning(f"JSON parse error: {e}")
        return {"status": "error", "message": f"Invalid JSON: {str(e)}"}
    
    ok, msg = _validate_body(body)
    if not ok:
        return {"status": "error", "message": msg}
    
    # Create future
    try:
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
    except Exception as e:
        logger.error(f"Failed to create future: {e}")
        return {"status": "error", "message": "Internal server error"}
    
    # Queue job
    try:
        job_q.put_nowait((body, fut))
    except asyncio.QueueFull:
        logger.warning("Queue full, rejecting request")
        return {"status": "error", "message": "Server busy, please retry later"}
    
    # Wait for result
    try:
        result = await asyncio.wait_for(fut, timeout=RESULT_TIMEOUT)
        return result
    except asyncio.TimeoutError:
        logger.warning(f"Request timeout for {body.get('site')}")
        return {"status": "error", "message": "Request timeout, please try again"}
    except Exception as e:
        logger.error(f"Error waiting for result: {e}")
        return {"status": "error", "message": "Internal server error"}

@app.get("/check")
async def check_card_get(site: str = Query(...), cc: str = Query(...)):
    """GET endpoint for card checking"""
    body = {"site": site, "cc": cc}
    
    ok, msg = _validate_body(body)
    if not ok:
        return {"status": "error", "message": msg}
    
    try:
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
    except Exception as e:
        logger.error(f"Failed to create future: {e}")
        return {"status": "error", "message": "Internal server error"}
    
    try:
        job_q.put_nowait((body, fut))
    except asyncio.QueueFull:
        logger.warning("Queue full, rejecting request")
        return {"status": "error", "message": "Server busy, please retry later"}
    
    try:
        result = await asyncio.wait_for(fut, timeout=RESULT_TIMEOUT)
        return result
    except asyncio.TimeoutError:
        logger.warning(f"Request timeout for {site}")
        return {"status": "error", "message": "Request timeout, please try again"}
    except Exception as e:
        logger.error(f"Error waiting for result: {e}")
        return {"status": "error", "message": "Internal server error"}
