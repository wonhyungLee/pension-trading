from fastapi.exception_handlers import (
    request_validation_exception_handler,
)
from pprint import pprint
from fastapi import FastAPI, Request, status, BackgroundTasks
from fastapi.responses import ORJSONResponse, RedirectResponse, FileResponse
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
import httpx
from datetime import datetime
from exchange.stock.kis import KoreaInvestment
from exchange.model import MarketOrder, PriceRequest, HedgeData, OrderRequest
from exchange.utility import (
    settings,
    log_order_message,
    log_alert_message,
    print_alert_message,
    logger_test,
    log_order_error_message,
    log_validation_error_message,
    log_hedge_message,
    log_error_message,
    log_message,
)
import traceback
from exchange import get_exchange, log_message, db, settings, get_bot, pocket
import ipaddress
import os
import sys
from devtools import debug
import asyncio
from dotenv import dotenv_values
from exchange.model.schemas import find_env_file
from pathlib import Path
import mimetypes
from site2_service import SITE2_ROUTER, site2_startup

VERSION = "0.1.3"
app = FastAPI(default_response_class=ORJSONResponse)
app.include_router(SITE2_ROUTER)

# 자산 모니터링 활성화 여부 (환경변수로 설정 가능)
ENABLE_ASSET_MONITOR = os.getenv("ENABLE_ASSET_MONITOR", "false").lower() == "true"
ASSET_REPORT_INTERVAL_HOURS = int(os.getenv("ASSET_REPORT_INTERVAL_HOURS", "6"))


def get_error(e):
    tb = traceback.extract_tb(e.__traceback__)
    target_folder = os.path.abspath(os.path.dirname(tb[0].filename))
    error_msg = []

    for tb_info in tb:
        # if target_folder in tb_info.filename:
        error_msg.append(
            f"File {tb_info.filename}, line {tb_info.lineno}, in {tb_info.name}"
        )
        error_msg.append(f"  {tb_info.line}")

    error_msg.append(str(e))

    return error_msg


@app.on_event("startup")
async def startup():
    log_message(f"POABOT 실행 완료! - 버전:{VERSION}")
    await site2_startup()
    
    # 자산 모니터링 시작
    if ENABLE_ASSET_MONITOR and settings.DISCORD_WEBHOOK_URL:
        try:
            from asset_monitor import run_periodic_asset_report
            log_message(f"자산 모니터링 시작 - {ASSET_REPORT_INTERVAL_HOURS}시간 간격")
            asyncio.create_task(run_periodic_asset_report(ASSET_REPORT_INTERVAL_HOURS))
        except Exception as e:
            log_message(f"자산 모니터링 모듈 로드 실패: {str(e)}")


@app.on_event("shutdown")
def shutdown():
    db.close()


whitelist = [
    "52.89.214.238",
    "34.212.75.30",
    "54.218.53.128",
    "52.32.178.7",
    "127.0.0.1",
]
whitelist = whitelist + settings.WHITELIST


# @app.middleware("http")
# async def add_process_time_header(request: Request, call_next):
#     start_time = time.perf_counter()
#     response = await call_next(request)
#     process_time = time.perf_counter() - start_time
#     response.headers["X-Process-Time"] = str(process_time)
#     return response


@app.middleware("http")
async def whitelist_middleware(request: Request, call_next):
    try:
        # Force HTTPS for the public portal domain (KakaoTalk link previews may prefer HTTPS).
        # Keep ACME HTTP-01 challenge path on HTTP for cert renewal.
        host = (request.headers.get("host") or "").split(":")[0].lower()
        if (
            host == "gbk.o-r.kr"
            and request.url.scheme == "http"
            and not request.url.path.startswith("/.well-known/acme-challenge/")
        ):
            return RedirectResponse(
                url=str(request.url.replace(scheme="https")),
                status_code=status.HTTP_308_PERMANENT_REDIRECT,
            )

        # Publicly serve the dashboard (SPA). Keep trading APIs protected by IP whitelist.
        path = request.url.path
        if request.method in ("GET", "HEAD"):
            protected_get_paths = {
                "/assets",
                "/ip",
                "/hi",
                "/docs",
                "/redoc",
                "/openapi.json",
            }
            protected_get_prefixes = ("/balance/",)

            if not (
                path in protected_get_paths
                or any(path.startswith(prefix) for prefix in protected_get_prefixes)
            ):
                return await call_next(request)

        # Allow site2 authentication endpoints for public access.
        # site2 data APIs remain protected by token auth in site2_service.
        if request.method in ("POST", "OPTIONS"):
            if (
                path.startswith("/site2/api/auth/")
                or path.startswith("/site2/api/api/auth/")
            ):
                return await call_next(request)

        if (
            request.client.host not in whitelist
            and not ipaddress.ip_address(request.client.host).is_private
        ):
            msg = f"{request.client.host}는 안됩니다"
            print(msg)
            return ORJSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content=f"{request.client.host}는 허용되지 않습니다",
            )
    except:
        log_error_message(traceback.format_exc(), "미들웨어 에러")
    else:
        response = await call_next(request)
        return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    msgs = [
        f"[에러{index+1}] " + f"{error.get('msg')} \n{error.get('loc')}"
        for index, error in enumerate(exc.errors())
    ]
    message = "[Error]\n"
    for msg in msgs:
        message = message + msg + "\n"

    log_validation_error_message(f"{message}\n {exc.body}")
    return await request_validation_exception_handler(request, exc)


@app.get("/ip")
async def get_ip():
    data = httpx.get("https://ipv4.jsonip.com").json()["ip"]
    log_message(data)


@app.get("/hi")
async def welcome():
    return "hi!!"


@app.get("/assets")
async def get_assets():
    """자산 현황 즉시 조회 API"""
    from asset_monitor import AssetMonitor
    monitor = AssetMonitor()
    
    crypto_assets = await monitor.get_crypto_assets()
    stock_assets = await monitor.get_stock_assets()
    
    return {
        "crypto": crypto_assets,
        "stock": stock_assets,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/balance/{exchange_name}")
async def get_balance(exchange_name: str):
    """특정 거래소의 자산 현황 조회 API"""
    try:
        bot = get_bot(exchange_name.upper())
        balance = bot.get_balance()
        return balance
    except Exception as e:
        error_msg = get_error(e)
        log_error_message("\n".join(error_msg), f"{exchange_name} 자산 조회 에러")
        return ORJSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "\n".join(error_msg)},
        )


@app.post("/assets/report")
async def send_asset_report():
    """자산 현황 리포트 즉시 전송"""
    from asset_monitor import AssetMonitor
    monitor = AssetMonitor()
    
    await monitor.report_assets()
    return {"result": "success", "message": "자산 현황 리포트가 전송되었습니다."}


@app.post("/price")
async def price(price_req: PriceRequest, background_tasks: BackgroundTasks):
    exchange = get_exchange(price_req.exchange)
    price = exchange.dict()[price_req.exchange].fetch_price(
        price_req.base, price_req.quote
    )
    return price


def log(exchange_name, result, order_info):
    log_order_message(exchange_name, result, order_info)
    print_alert_message(order_info)


def log_error(error_message, order_info):
    log_order_error_message(error_message, order_info)
    log_alert_message(order_info, "실패")


@app.post("/order")
@app.post("/")
@app.post("/trading")
async def order(order_info: MarketOrder, background_tasks: BackgroundTasks):
    order_result = None
    try:
        exchange_name = order_info.exchange
        
        # KIS 거래 시 kis_number가 없으면 .env 기준 기본값 할당
        if order_info.is_stock and order_info.kis_number is None:
            env_path = find_env_file()
            if env_path:
                env_vars = dotenv_values(env_path)
                kis_numbers_in_env = sorted([
                    int(k.replace('KIS', '').replace('_ACCOUNT_NUMBER', ''))
                    for k in env_vars.keys()
                    if k.startswith('KIS') and k.endswith('_ACCOUNT_NUMBER') and env_vars[k]
                ])
                if kis_numbers_in_env:
                    order_info.kis_number = kis_numbers_in_env[0]
                else:
                    raise ValueError(".env 파일에 설정된 KIS 계좌가 없습니다.")
            else:
                raise ValueError(".env 파일을 찾을 수 없습니다.")

        bot = get_bot(exchange_name, order_info.kis_number)
        bot.init_info(order_info)

        if bot.order_info.is_crypto:
            if bot.order_info.is_entry:
                order_result = bot.market_entry(bot.order_info)
            elif bot.order_info.is_close:
                order_result = bot.market_close(bot.order_info)
            elif bot.order_info.is_buy:
                order_result = bot.market_buy(bot.order_info)
            elif bot.order_info.is_sell:
                order_result = bot.market_sell(bot.order_info)
            background_tasks.add_task(log, exchange_name, order_result, order_info)
        elif bot.order_info.is_stock:
            order_result = bot.create_order(
                bot.order_info.exchange,
                bot.order_info.base,
                order_info.type.lower(),
                order_info.side.lower(),
                order_info.amount,
            )
            background_tasks.add_task(log, exchange_name, order_result, order_info)

    except TypeError as e:
        error_msg = get_error(e)
        background_tasks.add_task(
            log_order_error_message, "\n".join(error_msg), order_info
        )

    except Exception as e:
        error_msg = get_error(e)
        background_tasks.add_task(log_error, "\n".join(error_msg), order_info)

    else:
        return {"result": "success"}

    finally:
        pass


def get_hedge_records(base):
    records = pocket.get_full_list("kimp", query_params={"filter": f'base = "{base}"'
    })
    binance_amount = 0.0
    binance_records_id = []
    upbit_amount = 0.0
    upbit_records_id = []
    for record in records:
        if record.exchange == "BINANCE":
            binance_amount += record.amount
            binance_records_id.append(record.id)
        elif record.exchange == "UPBIT":
            upbit_amount += record.amount
            upbit_records_id.append(record.id)

    return {
        "BINANCE": {"amount": binance_amount, "records_id": binance_records_id},
        "UPBIT": {"amount": upbit_amount, "records_id": upbit_records_id},
    }


@app.post("/hedge")
async def hedge(hedge_data: HedgeData, background_tasks: BackgroundTasks):
    exchange_name = hedge_data.exchange.upper()
    bot = get_bot(exchange_name)
    upbit = get_bot("UPBIT")

    base = hedge_data.base
    quote = hedge_data.quote
    amount = hedge_data.amount
    leverage = hedge_data.leverage
    hedge = hedge_data.hedge

    foreign_order_info = OrderRequest(
        exchange=exchange_name,
        base=base,
        quote=quote,
        side="entry/sell",
        type="market",
        amount=amount,
        leverage=leverage,
    )
    bot.init_info(foreign_order_info)
    if hedge == "ON":
        try:
            if amount is None:
                raise Exception("헷지할 수량을 요청하세요")
            binance_order_result = bot.market_entry(foreign_order_info)
            binance_order_amount = binance_order_result["amount"]
            pocket.create(
                "kimp",
                {
                    "exchange": "BINANCE",
                    "base": base,
                    "quote": quote,
                    "amount": binance_order_amount,
                },
            )
            if leverage is None:
                leverage = 1
            try:
                korea_order_info = OrderRequest(
                    exchange="UPBIT",
                    base=base,
                    quote="KRW",
                    side="buy",
                    type="market",
                    amount=binance_order_amount,
                )
                upbit.init_info(korea_order_info)
                upbit_order_result = upbit.market_buy(korea_order_info)
            except Exception as e:
                hedge_records = get_hedge_records(base)
                binance_records_id = hedge_records["BINANCE"]["records_id"]
                binance_amount = hedge_records["BINANCE"]["amount"]
                binance_order_result = bot.market_close(
                    OrderRequest(
                        exchange=exchange_name,
                        base=base,
                        quote=quote,
                        side="close/buy",
                        amount=binance_amount,
                    )
                )
                for binance_record_id in binance_records_id:
                    pocket.delete("kimp", binance_record_id)
                log_message(
                    "[헷지 실패] 업비트에서 에러가 발생하여 바이낸스 포지션을 종료합니다"
                )
            else:
                upbit_order_info = upbit.get_order(upbit_order_result["id"])
                upbit_order_amount = upbit_order_info["filled"]
                pocket.create(
                    "kimp",
                    {
                        "exchange": "UPBIT",
                        "base": base,
                        "quote": "KRW",
                        "amount": upbit_order_amount,
                    },
                )
                log_hedge_message(
                    exchange_name,
                    base,
                    quote,
                    binance_order_amount,
                    upbit_order_amount,
                    hedge,
                )

        except Exception as e:
            # log_message(f"{e}")
            background_tasks.add_task(
                log_error_message, traceback.format_exc(), "헷지 에러"
            )
            return {"result": "error"}
        else:
            return {"result": "success"}

    elif hedge == "OFF":
        try:
            records = pocket.get_full_list(
                "kimp", query_params={"filter": f'base = "{base}"'} 
            )
            binance_amount = 0.0
            binance_records_id = []
            upbit_amount = 0.0
            upbit_records_id = []
            for record in records:
                if record.exchange == "BINANCE":
                    binance_amount += record.amount
                    binance_records_id.append(record.id)
                elif record.exchange == "UPBIT":
                    upbit_amount += record.amount
                    upbit_records_id.append(record.id)

            if binance_amount > 0 and upbit_amount > 0:
                # 바이낸스
                order_info = OrderRequest(
                    exchange="BINANCE",
                    base=base,
                    quote=quote,
                    side="close/buy",
                    amount=binance_amount,
                )
                binance_order_result = bot.market_close(order_info)
                for binance_record_id in binance_records_id:
                    pocket.delete("kimp", binance_record_id)
                # 업비트
                order_info = OrderRequest(
                    exchange="UPBIT",
                    base=base,
                    quote="KRW",
                    side="sell",
                    amount=upbit_amount,
                )
                upbit_order_result = upbit.market_sell(order_info)
                for upbit_record_id in upbit_records_id:
                    pocket.delete("kimp", upbit_record_id)

                log_hedge_message(
                    exchange_name, base, quote, binance_amount, upbit_amount, hedge
                )
            elif binance_amount == 0 and upbit_amount == 0:
                log_message(f"{exchange_name}, UPBIT에 종료할 수량이 없습니다")
            elif binance_amount == 0:
                log_message(f"{exchange_name}에 종료할 수량이 없습니다")
            elif upbit_amount == 0:
                log_message("UPBIT에 종료할 수량이 없습니다")
        except Exception as e:
            background_tasks.add_task(
                log_error_message, traceback.format_exc(), "헷지종료 에러"
            )
            return {"result": "error"}
        else:
            return {"result": "success"}


# Serve the Central Platform Dashboard (Vite build output) from this FastAPI server.
# This allows direct access via server IP (port 80) without needing a separate web server.
_PORTAL_DIST_DIR = (
    Path(__file__).resolve().parent / "플랫폼사이트" / "central-portal" / "dist"
)
if _PORTAL_DIST_DIR.is_dir():
    # Ensure common asset types have correct Content-Type when served via StaticFiles.
    mimetypes.add_type("image/webp", ".webp")

    # Serve ACME HTTP-01 challenges for Let's Encrypt using webroot method.
    # certbot will place files under /var/www/certbot/.well-known/acme-challenge/
    _CERTBOT_WEBROOT_WELL_KNOWN = Path("/var/www/certbot/.well-known")
    if _CERTBOT_WEBROOT_WELL_KNOWN.is_dir():
        app.mount(
            "/.well-known",
            StaticFiles(directory=str(_CERTBOT_WEBROOT_WELL_KNOWN)),
            name="well-known",
        )

    _PORTAL_ASSETS_DIR = _PORTAL_DIST_DIR / "assets"
    if _PORTAL_ASSETS_DIR.is_dir():
        # Keep existing API routes like GET /assets, POST /assets/report working:
        # only /assets/* (files) will be served by StaticFiles.
        app.mount(
            "/assets",
            StaticFiles(directory=str(_PORTAL_ASSETS_DIR)),
            name="central-portal-assets",
        )

    @app.get("/", include_in_schema=False)
    async def central_portal_index():
        return FileResponse(_PORTAL_DIST_DIR / "index.html")

    @app.get("/favicon.svg", include_in_schema=False)
    async def central_portal_favicon():
        return FileResponse(_PORTAL_DIST_DIR / "favicon.svg")

    @app.get("/vite.svg", include_in_schema=False)
    async def central_portal_vite_logo():
        return FileResponse(_PORTAL_DIST_DIR / "vite.svg")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def central_portal_spa_fallback(full_path: str, request: Request):
        # SPA routing: direct navigation to /foo should serve index.html.
        return FileResponse(_PORTAL_DIST_DIR / "index.html")
