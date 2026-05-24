from fastapi import FastAPI, Request, Query
import asyncio, time, random
from typing import Dict, Any, Tuple
from asho import get_variant_and_token

app = FastAPI()

# --- Backpressure + global pacing ---
WORKERS = 10              # jaga 1 req/detik global; naikin kalau mau paralel, tapi pacing tetap 1/detik
RATE_DELAY = 2.0         # jeda minimal antar request ke upstream (rotator butuh ~1s)
RESULT_TIMEOUT = 360     # timeout nunggu 1 job
MAX_QUEUE = 20000        # batasi antrian biar gak makan RAM

job_q: "asyncio.Queue[Tuple[Dict[str, Any], asyncio.Future]]" = asyncio.Queue(maxsize=MAX_QUEUE)
_last_dispatch = 0.0
_pace_lock = asyncio.Lock()

async def _pace(delay: float = RATE_DELAY) -> None:
    global _last_dispatch
    async with _pace_lock:
        now = time.monotonic()
        wait = _last_dispatch + delay - now
        if wait > 0:
            await asyncio.sleep(wait)
        _last_dispatch = time.monotonic()

async def _worker(idx: int):
    while True:
        body, fut = await job_q.get()
        try:
            # Pacing global: kasih jarak >= RATE_DELAY antar request
            await _pace(RATE_DELAY)

            site = body["site"]
            cc = body["cc"]
            try:
                cc_number, cc_month, cc_year, cc_cvv = cc.split("|")
            except Exception:
                fut.set_result({"status": "error", "message": "Invalid CC format. Use card|mm|yy|cvv"})
                continue

            try:
                result = await get_variant_and_token(site, cc_number, cc_month, cc_year, cc_cvv)
                fut.set_result({"status": "success", "result": result})
            except Exception as e:
                fut.set_result({"status": "error", "message": str(e)})
        except Exception as e:
            fut.set_result({"status": "error", "message": f"worker-{idx}: {e}"})
        finally:
            job_q.task_done()

@app.on_event("startup")
async def _startup():
    for i in range(WORKERS):
        asyncio.create_task(_worker(i))

@app.get("/")
async def root():
    return {"status": "OK", "message": "Shopify Gate API working."}

def _validate_body(body: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(body, dict):
        return False, "Invalid JSON body"
    for k in ("site", "cc"):
        if k not in body:
            return False, f"Missing required parameter: {k}"
    return True, ""

@app.post("/check")
async def check_card_post(data: Request):
    body = await data.json()
    ok, msg = _validate_body(body)
    if not ok:
        return {"status": "error", "message": msg}
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    try:
        job_q.put_nowait((body, fut))
    except asyncio.QueueFull:
        return {"status": "error", "message": "Server busy, please retry later"}
    try:
        return await asyncio.wait_for(fut, timeout=RESULT_TIMEOUT)
    except asyncio.TimeoutError:
        return {"status": "error", "message": "Timeout while processing request"}

@app.get("/check")
async def check_card_get(site: str = Query(...), cc: str = Query(...)):
    body = {"site": site, "cc": cc}
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    try:
        job_q.put_nowait((body, fut))
    except asyncio.QueueFull:
        return {"status": "error", "message": "Server busy, please retry later"}
    try:
        return await asyncio.wait_for(fut, timeout=RESULT_TIMEOUT)
    except asyncio.TimeoutError:
        return {"status": "error", "message": "Timeout while processing request"}