import asyncio
from datetime import datetime
from domain.entities import Analysis
from domain.value_objects import Timeframe
from application.events import EventBus, CandleReceived
from application.config import SessionConfig
from utilities.indicators import get_indicator_analyzers
from utilities.candle_patterns import get_candle_pattern_analyzers
from utilities.market_patterns import get_market_pattern_analyzers
from presentation.cli_logger import log_analysis
import uuid

# tracks how many M1 candles received per symbol
# used to decide when a higher timeframe bar has closed
_bar_counters: dict[str, int] = {}

_TIMEFRAME_MINUTES = {
    "M1": 1, "M5": 5, "M15": 15,
    "M30": 30, "H1": 60, "D1": 1440
}


class PatternService:
    def __init__(self, candle_repo, analysis_repo, event_bus: EventBus, config: SessionConfig):
        self.candle_repo = candle_repo
        self.analysis_repo = analysis_repo
        self.config = config
        event_bus.subscribe(CandleReceived, self.on_candle_received)

    async def on_candle_received(self, event: CandleReceived):
        key = event.symbol
        count = _bar_counters.get(key, 0) + 1
        _bar_counters[key] = count

        # find which timeframes just closed
        closed_tfs = [
            tf for tf in self._all_timeframes()
            if count % _TIMEFRAME_MINUTES[tf] == 0
        ]

        if not closed_tfs:
            return

        # run analysis for each closed timeframe in parallel
        tasks = [
            self._run_for_timeframe(event.symbol, tf)
            for tf in closed_tfs
        ]
        await asyncio.gather(*tasks)

    async def _run_for_timeframe(self, symbol: str, timeframe: str):
        candles = self.candle_repo.get_latest(
            symbol, timeframe, self.config.candle_window)

        if not candles:
            return

        # build analyzer list from config — only what's requested for this timeframe
        analyzers = self._build_analyzers(timeframe)

        analysis = Analysis(
            session_id=str(uuid.uuid4()),
            timeframe=Timeframe(timeframe),
            timestamp=datetime.utcnow(),
            candles_window=len(candles),
        )
        analysis.build(candles, analyzers)

        self.analysis_repo.save(symbol, analysis)
        log_analysis(analysis)

    def _build_analyzers(self, timeframe: str) -> list:
        analyzers = []
        cfg = self.config

        if timeframe in cfg.timeframes.candle_patterns:
            analyzers += get_candle_pattern_analyzers(
                cfg.analyzers.candle_patterns)

        if timeframe in cfg.timeframes.market_patterns:
            analyzers += get_market_pattern_analyzers(
                cfg.analyzers.market_patterns)

        if timeframe in cfg.timeframes.indicators:
            analyzers += get_indicator_analyzers(cfg.analyzers.indicators)

        return analyzers

    def _all_timeframes(self) -> list[str]:
        cfg = self.config.timeframes
        return list(set(
            cfg.candle_patterns + cfg.market_patterns + cfg.indicators
        ))
