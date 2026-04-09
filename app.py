import asyncio
import os
import threading

from flask import Flask, jsonify
from flask_cors import CORS
from pyngrok import ngrok

from infrastructure.integrations.deriv import DerivClient
from infrastructure.integrations.firebase import FirebaseClient
from infrastructure.repositories.candle_repo import CandleRepository
from infrastructure.repositories.analysis_repo import AnalysisRepository
from application.events import EventBus
from application.services.candle_service import CandleService
from application.services.pattern_service import PatternService
from application.config import SessionConfig, TimeframeConfig, AnalyzerConfig

# ── config ────────────────────────────────────────────────────
config = SessionConfig(
    markets=["frxEURUSD", "R_25"],
    timeframes=TimeframeConfig(
        candle_patterns=["M5", "M15", "H1"],
        market_patterns=["M15", "H1"],
        indicators=["M5", "H1", "D1"],
    ),
    analyzers=AnalyzerConfig(
        candle_patterns=["rising_three", "morning_star", "engulfing"],
        market_patterns=["double_top", "rising_staircase"],
        indicators=["rsi", "macd", "bollinger_bands"],
    ),
    candle_window=220,
)

# ── shared infra (initialised in main, used by Flask routes) ──
candle_repo: CandleRepository = None
analysis_repo: AnalysisRepository = None

# ── flask app ─────────────────────────────────────────────────
flask_app = Flask(__name__)
CORS(flask_app)

@flask_app.route("/health")
def health():
    return jsonify({"status": "ok"})

@flask_app.route("/candles/<symbol>")
def get_candles(symbol):
    data = candle_repo.get_recent(symbol)
    return jsonify(data)

@flask_app.route("/analysis/<symbol>")
def get_analysis(symbol):
    data = analysis_repo.get_latest(symbol)
    return jsonify(data)

# ── streamer ──────────────────────────────────────────────────
async def main():
    global candle_repo, analysis_repo

    # ── infra ─────────────────────────────────────────────────
    deriv = DerivClient()
    firebase = FirebaseClient(credential_path="firebase_creds.json")
    await deriv.connect()

    candle_repo = CandleRepository(firebase)
    analysis_repo = AnalysisRepository(firebase)

    # ── event bus ─────────────────────────────────────────────
    bus = EventBus()

    # ── services ──────────────────────────────────────────────
    candle_svc = CandleService(deriv, candle_repo, bus)
    pattern_svc = PatternService(candle_repo, analysis_repo, bus, config)  # noqa: F841

    # ── bootstrap historic data ────────────────────────────────
    for symbol in config.markets:
        print(f"Bootstrapping {symbol}...")
        await candle_svc.bootstrap(symbol, "M1", count=1000)

    # ── start live streams ─────────────────────────────────────
    streams = [
        candle_svc.start(symbol, "M1")
        for symbol in config.markets
    ]
    await asyncio.gather(*streams)

# ── entrypoint ────────────────────────────────────────────────
if __name__ == "__main__":
    # start streamer in background thread so Flask can run on main thread
    def run_streamer():
        asyncio.run(main())

    streamer_thread = threading.Thread(target=run_streamer, daemon=True)
    streamer_thread.start()

    # ngrok tunnel
    ngrok_token = os.environ.get("NGROK_TOKEN")
    if ngrok_token:
        ngrok.set_auth_token(ngrok_token)
        public_url = ngrok.connect(5000)
        print(f"Public API URL: {public_url}")
    else:
        print("NGROK_TOKEN not set — skipping tunnel, API on http://localhost:5000")

    # Flask blocks here, keeping the process alive
    flask_app.run(host="0.0.0.0", port=5000)
