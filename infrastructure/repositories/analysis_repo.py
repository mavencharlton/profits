from domain.entities import Analysis
from domain.value_objects import Timeframe
from datetime import datetime


class AnalysisRepository:
    def __init__(self, firebase):
        self.db = firebase

    def save(self, symbol: str, analysis: Analysis):
        doc_id = f"{symbol}_{analysis.timeframe.value}_{int(analysis.timestamp.timestamp())}"
        self.db.save("analysis", doc_id, self._to_dict(symbol, analysis))

    def get_latest(self, symbol: str, timeframe: str, count: int = 1) -> list[Analysis]:
        raw = self.db.query("analysis", [
            ("symbol",    "==", symbol),
            ("timeframe", "==", timeframe),
        ])
        raw_sorted = sorted(raw, key=lambda r: r["timestamp"])[-count:]
        return [self._from_dict(r) for r in raw_sorted]

    def get_latest_all_timeframes(self, symbol: str) -> dict[str, Analysis]:
        """Returns most recent analysis per timeframe for a symbol."""
        raw = self.db.query("analysis", [("symbol", "==", symbol)])
        by_tf: dict[str, dict] = {}
        for r in raw:
            tf = r["timeframe"]
            if tf not in by_tf or r["timestamp"] > by_tf[tf]["timestamp"]:
                by_tf[tf] = r
        return {tf: self._from_dict(r) for tf, r in by_tf.items()}

    @staticmethod
    def _to_dict(symbol: str, analysis: Analysis) -> dict:
        return {
            "symbol":         symbol,
            "session_id":     analysis.session_id,
            "timeframe":      analysis.timeframe.value,
            "timestamp":      analysis.timestamp.isoformat(),
            "candles_window": analysis.candles_window,
            "data":           analysis.data,
            "narrative":      analysis.narrative,
        }

    @staticmethod
    def _from_dict(raw: dict) -> Analysis:
        analysis = Analysis(
            session_id=raw["session_id"],
            timeframe=Timeframe(raw["timeframe"]),
            timestamp=datetime.fromisoformat(raw["timestamp"]),
            candles_window=raw["candles_window"],
        )
        analysis.data = raw.get("data", {})
        analysis.narrative = raw.get("narrative", "")
        return analysis
