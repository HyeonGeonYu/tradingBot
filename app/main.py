# app/main.py

import os
import sys
import time
import asyncio
import logging
from typing import Literal

from dotenv import load_dotenv
load_dotenv()

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, Response, HTTPException
from pydantic import BaseModel

# 새 구조에 맞춘 import
from core.trade_bot import TradeBot                # ← core.trade_bot 에서 변경
from controllers.controller import (
    BybitWebSocketController,
    BybitRestController,
)
from asyncio import Queue
from utils.logger import setup_logger

class ManualOrderRequest(BaseModel):
    percent: float = 10
    symbol: str | None = None

class ManualCloseRequest(BaseModel):
    side: Literal["LONG", "SHORT"]
    symbol: str | None = None

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ── 로거 설정 ───────────────────────────────────
system_logger = setup_logger(
    "system",
    logger_level=logging.DEBUG,
    console_level=logging.DEBUG,
    file_level=logging.INFO,
    enable_telegram=True,
    telegram_level=logging.INFO,
    exclude_sig_in_file=False,
    telegram_mode="both",
)

trading_logger = setup_logger(
    "trading",
    logger_level=logging.DEBUG,
    console_level=logging.DEBUG,
    file_level=logging.INFO,
    enable_telegram=True,
    telegram_level=logging.INFO,
    write_signals_file=True,
    signals_filename="signals.jsonl",
    exclude_sig_in_file=False,
    telegram_mode="both",
)

# ── FastAPI 앱 ───────────────────────────────────
app = FastAPI()
manual_queue: Queue = Queue()

bot: TradeBot | None = None
bybit_websocket_controller: BybitWebSocketController | None = None
bybit_rest_controller: BybitRestController | None = None


async def warmup_with_ws_prices():
    """
    새 구조에선 TradeBot.record_price가 없으므로,
    워밍업 동안 WS에서 직접 가격을 읽어 JumpDetector에 채운다.
    """
    assert bot is not None and bybit_websocket_controller is not None
    MIN_TICKS = bot.jump.history_num

    while True:
        try:
            # 각 심볼 가격을 가져와 jump 히스토리에 채움
            missing: dict[str, int] = {}
            for sym in bot.symbols:
                price = bybit_websocket_controller.get_price(sym)
                if price:
                    bot.jump.record_price(sym, price, ts=time.time())
                cur = len(bot.jump.price_history.get(sym, []))
                if cur < MIN_TICKS:
                    missing[sym] = cur

            if not missing:  # 모든 심볼이 MIN_TICKS 충족
                system_logger.debug("✅ 데이터 준비 완료, 메인 루프 시작")
                return

            system_logger.debug(f"⏳ 데이터 준비 중... (부족: {missing})")
            await asyncio.sleep(0.5)

        except Exception as e:
            system_logger.error(f"❌ warmup 오류: {e}")
            await asyncio.sleep(1.0)


async def bot_loop():
    assert bot is not None
    # 1) 워밍업: 가격 샘플이 충분히 쌓일 때까지 대기
    await warmup_with_ws_prices()

    # 2) 메인 루프
    while True:
        try:
            await bot.run_once()
            await asyncio.sleep(0.5)
        except Exception as e:
            system_logger.error(f"❌ bot_loop 오류: {e}")
            await asyncio.sleep(10)


@app.on_event("startup")
async def startup_event():
    global bot, bybit_websocket_controller, bybit_rest_controller
    system_logger.debug("🚀 FastAPI 기반 봇 서버 시작")

    symbols = ("BTCUSDT","ETHUSDT","XAUTUSDT")
    # symbols = ("BTCUSDT",)

    # WS/REST 컨트롤러 초기화 (중복 생성 제거)
    bybit_websocket_controller = BybitWebSocketController(
        symbols=symbols,
        system_logger=system_logger
    )
    bybit_rest_controller = BybitRestController(system_logger=system_logger)

    # TradeBot 초기화 (새 구조)
    bot = TradeBot(
        bybit_websocket_controller,
        bybit_rest_controller,
        manual_queue,
        system_logger=system_logger,
        trading_logger=trading_logger,
        symbols=symbols,
    )

    # 백그라운드 루프 시작
    asyncio.create_task(bot_loop())


@app.get("/info")
async def status(symbol: str = "BTCUSDT", plain: bool = True):
    if bot is None:
        raise HTTPException(status_code=503, detail="Bot not initialized yet")
    if symbol not in bot.symbols:
        raise HTTPException(status_code=400, detail=f"Unknown symbol: {symbol}. Available: {bot.symbols}")

    status_text = bot.make_status_log_msg(symbol)
    if plain:
        return Response(content=status_text, media_type="text/plain")
    return {"symbol": symbol, "message": status_text}


# 필요하면 수동 엔드포인트를 다시 열어 사용하세요.
# @app.post("/long")
# async def manual_buy(request: ManualOrderRequest):
#     await manual_queue.put({"command": "long", "percent": request.percent, "symbol": request.symbol})
#     return {"status": f"long triggered with {request.percent}%", "symbol": request.symbol}
#
# @app.post("/short")
# async def manual_sell(request: ManualOrderRequest):
#     await manual_queue.put({"command": "short", "percent": request.percent, "symbol": request.symbol})
#     return {"status": f"short triggered with {request.percent}%", "symbol": request.symbol}
#
# @app.post("/close")
# async def manual_close(request: ManualCloseRequest):
#     await manual_queue.put({"command": "close", "side": request.side, "symbol": request.symbol})
#     return {"status": f"close triggered for {request.side}", "symbol": request.symbol}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=False)
