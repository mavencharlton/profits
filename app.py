import asyncio
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


async def main():
    # ── infra ─────────────────────────────────────────────────
    deriv = DerivClient(app_id="your_app_id")
    firebase = FirebaseClient(credential_path="firebase_creds.json")
    await deriv.connect()

    candle_repo = CandleRepository(firebase)
    analysis_repo = AnalysisRepository(firebase)

    # ── event bus ─────────────────────────────────────────────
    bus = EventBus()

    # ── services ──────────────────────────────────────────────
    candle_svc = CandleService(deriv, candle_repo, bus)
    pattern_svc = PatternService(candle_repo, analysis_repo, bus, config)

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

if __name__ == "__main__":
    asyncio.run(main())
