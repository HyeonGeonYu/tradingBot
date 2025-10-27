# execution.py
import asyncio, time
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Asia/Seoul")

class ExecutionEngine:
    """주문 실행 + 체결 대기 + 상태 동기화 + 손익 로그"""
    def __init__(self, rest, system_logger=None, trading_logger=None, taker_fee_rate: float = 0.00055):
        self.rest = rest
        self.system_logger = system_logger
        self.trading_logger = trading_logger
        self.TAKER_FEE_RATE = taker_fee_rate
        self._sync_lock = asyncio.Lock()
        self._just_traded_until = 0.0

    async def execute_and_sync(self, fn, prev_status, symbol, *args, **kwargs):
        async with self._sync_lock:
            try:
                result = fn(*args, **kwargs)
            except Exception as e:
                if self.system_logger: self.system_logger.error(f"❌ 주문 실행 예외: {e}")
                return None

            if not result or not isinstance(result, dict):
                if self.system_logger: self.system_logger.warning("⚠️ 주문 결과가 비었습니다(또는 dict 아님).")
                return result

            order_id = result.get("orderId")
            if not order_id:
                if self.system_logger: self.system_logger.warning("⚠️ orderId 없음 → 체결 대기 스킵")
                return result

            filled = self.rest.wait_order_fill(symbol, order_id)
            orderStatus = (filled or {}).get("orderStatus", "").upper()

            if orderStatus == "FILLED":
                self._log_fill(filled, prev_status=prev_status)
                self.rest.set_full_position_info(symbol)
                trade = self.rest.get_trade_w_order_id(symbol, order_id)
                if trade:
                    self.rest.append_order(symbol, trade)
                self.rest.set_wallet_balance()
                if self.system_logger:
                    self.system_logger.info(f"🧾 체결 동기화 완료: {order_id[-6:]}")
            elif orderStatus in ("CANCELLED", "REJECTED"):
                if self.system_logger: self.system_logger.warning(f"⚠️ 주문 {order_id[-6:]} 상태: {orderStatus} (체결 없음)")
            elif orderStatus == "TIMEOUT":
                if self.system_logger: self.system_logger.warning(f"⚠️ 주문 {order_id[-6:]} 체결 대기 타임아웃 → 취소 시도")
                try:
                    cancel_res = self.rest.cancel_order(symbol, order_id)
                    if self.system_logger: self.system_logger.warning(f"🗑️ 취소 결과: {cancel_res}")
                except Exception as e:
                    if self.system_logger: self.system_logger.error(f"단일 주문 취소 실패: {e}")
            else:
                if self.system_logger: self.system_logger.warning(f"ℹ️ 주문 {order_id[-6:]} 상태: {orderStatus or 'UNKNOWN'}")

            self._just_traded_until = time.monotonic() + 0.8
            return result

    # --- 체결 로그 & 손익 ---
    def _classify_intent(self, filled: dict) -> Optional[str]:
        side = (filled.get("side") or "").upper()   # BUY/SELL
        pos  = int(filled.get("positionIdx") or 0)  # 1/2
        ro   = bool(filled.get("reduceOnly"))
        if ro:
            if pos == 1 and side == "SELL":  return "LONG_CLOSE"
            if pos == 2 and side == "BUY":   return "SHORT_CLOSE"
        else:
            if pos == 1 and side == "BUY":   return "LONG_OPEN"
            if pos == 2 and side == "SELL":  return "SHORT_OPEN"
        return None

    def _log_fill(self, filled: dict, prev_status: dict | None = None):
        intent = self._classify_intent(filled)
        if not intent: return
        side, action = intent.split("_")
        order_tail = (filled.get("orderId") or "")[-6:] or "UNKNOWN"
        avg_price = float(filled.get("avgPrice") or 0.0)
        exec_qty  = float(filled.get("cumExecQty") or filled.get("qty") or 0.0)

        if not action.endswith("CLOSE"):
            if self.trading_logger:
                self.trading_logger.info(
                    f"✅ {side} 주문 체결 완료 | id:{order_tail} | avg:{avg_price:.2f} | qty:{exec_qty}"
                )
            return

        entry_price = self._extract_entry_price_from_prev(filled, prev_status)
        if entry_price is None: entry_price = avg_price  # 방어적

        if (side, action) == ("LONG", "CLOSE"):
            profit_gross = (avg_price - entry_price) * exec_qty
        else:
            profit_gross = (entry_price - avg_price) * exec_qty

        total_fee = (entry_price * exec_qty + avg_price * exec_qty) * self.TAKER_FEE_RATE
        profit_net = profit_gross - total_fee
        profit_rate = (profit_gross / entry_price) * 100 if entry_price else 0.0

        if self.trading_logger:
            self.trading_logger.info(
                f"✅ {side} 청산 | id:{order_tail} | entry:{entry_price:.2f} / close:{avg_price:.2f} | "
                f"qty:{exec_qty} | PnL(net):{profit_net:.2f} | gross:{profit_gross:.2f}, fee:{total_fee:.2f} | "
                f"rate:{profit_rate:.2f}%"
            )

    def _extract_entry_price_from_prev(self, filled: dict, prev_status: dict | None) -> float | None:
        if not prev_status: return None
        pos_idx = int(filled.get("positionIdx") or 0)  # 1: LONG, 2: SHORT
        side_key = "LONG" if pos_idx == 1 else "SHORT"
        positions = prev_status.get("positions") or []
        for p in positions:
            if (p.get("position") or "").upper() == side_key:
                ep = p.get("entryPrice") or p.get("avgPrice")
                if ep is not None:
                    try: return float(ep)
                    except: pass
                entries = p.get("entries") or []
                if entries:
                    try: return float(entries[-1][2])  # (ts, qty, price, t_str)
                    except: pass
        return None
