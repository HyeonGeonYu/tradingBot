import logging, os, json, html, requests
from pathlib import Path

class OnlySIG(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            return isinstance(msg, str) and msg.lstrip().startswith("SIG ")
        except Exception:
            return False

class ExcludeSIG(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            return not (isinstance(msg, str) and msg.lstrip().startswith("SIG "))
        except Exception:
            return True

def send_telegram_message(bot_token: str, chat_id: str, message: str):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, data={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=10).raise_for_status()


class TelegramLogHandler(logging.Handler):
    def __init__(self, bot_token: str, chat_id: str, level=logging.WARNING):
        super().__init__(level)
        self.bot_token = bot_token
        self.chat_id = chat_id

    def emit(self, record):
        try:
            msg = record.getMessage()
            # SIG JSON 라인을 텔레그램용으로 예쁘게 변환
            if isinstance(msg, str) and msg.lstrip().startswith("SIG "):
                try:
                    obj = json.loads(msg.split(" ", 1)[1])
                    kind = obj.get("kind"); side = obj.get("side")
                    price = obj.get("price"); ma100 = obj.get("ma100")
                    d_pct = obj.get("ma_delta_pct") or 0; mom = obj.get("momentum_pct") or 0
                    th = obj.get("thresholds", {}); ts = obj.get("ts")
                    badge = "🟢" if (kind=="ENTRY" and side=="LONG") else \
                            "🔴" if (kind=="ENTRY" and side=="SHORT") else \
                            "🔵" if (kind=="EXIT"  and side=="LONG") else "🟣"
                    title = "진입" if kind=="ENTRY" else "청산"
                    side_kr = "롱" if side=="LONG" else "숏"
                    text = (
                        f"{badge} <b>{side_kr} {title} 신호</b>\n"
                        f"• 가격: <code>{price:,.2f}</code>\n"
                        f"• MA100: <code>{ma100:,.2f}</code> (Δ <code>{d_pct*100:+.2f}%</code>)\n"
                        f"• 모멘텀(3분): <code>{mom*100:+.2f}%</code>\n"
                        f"• 임계값: MA <code>±{th.get('ma',0)*100:.2f}%</code>, "
                        f"모멘텀 <code>{th.get('momentum',0)*100:.2f}%</code>\n"
                        f"• 시간: <i>{html.escape(str(ts))}</i>"
                    )
                    send_telegram_message(self.bot_token, self.chat_id, text)
                    return
                except Exception:
                    pass
            # 일반 라인은 포맷 그대로
            send_telegram_message(self.bot_token, self.chat_id, self.format(record))
        except Exception as e:
            print(f"TelegramLogHandler Error: {e}")

def _project_root(start_file: str = __file__) -> Path:
    """파일 위치에서 위로 올라가며 프로젝트 루트를 추정(.git/pyproject/requirements 기준)."""
    p = Path(start_file).resolve()
    for parent in [p.parent] + list(p.parents):
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists() or (parent / "requirements.txt").exists():
            return parent
    return p.parents[1]  # fallback: 파일 기준 한 단계 위

def setup_logger(
    logger_name: str,
    *,
    logger_level: int = logging.DEBUG,
    console_level=logging.DEBUG,
    file_level=logging.INFO,
    enable_telegram: bool = True,
    telegram_level: int = logging.INFO,
    write_signals_file: bool = False,             # ✅ trading 전용으로만 True
    signals_filename: str = "signals.jsonl",
    exclude_sig_in_file: bool = True,           # ✅ 추가
    telegram_mode: str = "sig_only",            # ✅ 추가: 'sig_only' | 'human_only' | 'both'
) -> logging.Logger:
    root = _project_root(__file__)
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)
    logger.propagate = False                     # ✅ 상위 전파 차단

    # ✅ 중복 방지: 기존 핸들러 제거
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # 포맷
    human_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")

    # 콘솔
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(human_fmt)
    logger.addHandler(ch)

    # 파일(사람용): SIG 제외
    fh = logging.FileHandler(log_dir / f"{logger_name}.log", encoding="utf-8")
    fh.setLevel(file_level)
    fh.setFormatter(human_fmt)
    if exclude_sig_in_file:  # ✅ 옵션에 따라 SIG 제외
        fh.addFilter(ExcludeSIG())
    logger.addHandler(fh)

    # 파일(기계용): 이 로거에만 signals.jsonl 사용하고 싶을 때
    if write_signals_file:
        fh_sig = logging.FileHandler(log_dir / signals_filename, encoding="utf-8")
        fh_sig.setLevel(logging.INFO)
        fh_sig.setFormatter(logging.Formatter("%(message)s"))  # JSON 그대로
        fh_sig.addFilter(OnlySIG())
        logger.addHandler(fh_sig)

    # 텔레그램
    if enable_telegram:
        bot = os.getenv("TELEGRAM_BOT_TOKEN")
        chat = os.getenv("TELEGRAM_CHAT_ID")
        if bot and chat:
            th = TelegramLogHandler(bot, chat, level=telegram_level)
            th.setFormatter(logging.Formatter("%(message)s"))
            if telegram_mode == "sig_only":
                th.addFilter(OnlySIG())
            elif telegram_mode == "human_only":
                th.addFilter(ExcludeSIG())
            elif telegram_mode == "both":
                pass  # 필터 없음 → 둘 다
            else:
                # 잘못된 값이면 안전하게 사람용만
                th.addFilter(ExcludeSIG())
            logger.addHandler(th)

    return logger
