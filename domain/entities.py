from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from .value_objects import (
    CandleType, CandleTypes, Timeframe,
    Direction, SignalStatus, ContractType, Sentiment
)


@dataclass
class Candle:
    open:        float
    high:        float
    low:         float
    close:       float
    volume:      float
    timestamp:   datetime
    candle_type: Optional[CandleType] = field(default=None, init=False)

    def classify(self, classifiers: list) -> None:
        """
        classifiers: list of callables (candle) -> CandleType | None
        First match wins. Falls back to UNKNOWN.
        """
        for classifier in classifiers:
            result = classifier(self)
            if result is not None:
                self.candle_type = result
                return
        self.candle_type = CandleTypes.UNKNOWN


@dataclass(frozen=True)
class Market:
    symbol: str
    name:   str

    def __str__(self):
        return f"{self.name} ({self.symbol})"


@dataclass
class Analysis:
    session_id:     str
    timeframe:      Timeframe
    timestamp:      datetime
    candles_window: int
    data:           dict = field(default_factory=dict)
    narrative:      str = ""

    def build(self, candles: list[Candle], analyzers: list) -> None:
        """
        analyzers: list of callables (candles) -> dict
        All results merged into self.data.
        """
        for analyzer in analyzers:
            result = analyzer(candles)
            if isinstance(result, dict):
                self.data.update(result)
        self.narrative = self._build_narrative()

    def _build_narrative(self) -> str:
        parts = []
        if "trend" in self.data:
            parts.append(f"Trend: {self.data['trend']}")
        if "rsi" in self.data:
            parts.append(f"RSI: {self.data['rsi']}")
        if "macd_signal" in self.data:
            parts.append(f"MACD: {self.data['macd_signal']}")
        if "squeeze" in self.data:
            parts.append(
                "Squeeze active." if self.data["squeeze"] else "No squeeze.")
        if "bos" in self.data:
            parts.append(f"BOS: {self.data['bos']}")
        if "choch" in self.data:
            parts.append(f"CHoCH: {self.data['choch']}")
        if "candle_patterns" in self.data:
            parts.append(
                f"Patterns: {', '.join(self.data['candle_patterns'])}")
        if "market_patterns" in self.data:
            parts.append(f"Market: {', '.join(self.data['market_patterns'])}")
        return " | ".join(parts) if parts else "No analysis available."

    def to_dict(self) -> dict:
        return {
            "session_id":     self.session_id,
            "timeframe":      self.timeframe.value,
            "timestamp":      self.timestamp.isoformat(),
            "candles_window": self.candles_window,
            "data":           self.data,
            "narrative":      self.narrative,
        }


@dataclass
class Trade:
    id:            str
    market:        Market
    contract_type: ContractType
    stake:         float
    duration:      int
    entry_time:    datetime
    entry_trigger: str
    signal_ids:    list[str] = field(default_factory=list)
    outcome:       Optional[str] = None
    payout:        Optional[float] = None
    exit_time:     Optional[datetime] = None
    review:        Optional[dict] = None

    def do_review(self, rules: list) -> None:
        """
        rules: list of callables (trade) -> dict | None
        Populates self.review. Does not block trading.
        """
        violations = []
        passed = []

        for rule in rules:
            result = rule(self)
            if result is not None:
                violations.append(result)
            else:
                passed.append(rule.__name__)

        score = max(0, 100 - sum(
            30 if v["severity"] == "high" else
            15 if v["severity"] == "medium" else 5
            for v in violations
        ))

        self.review = {
            "score":          score,
            "violations":     violations,
            "good_practices": passed,
            "summary":        _review_summary(score, violations),
        }

    def to_dict(self) -> dict:
        return {
            "id":            self.id,
            "market":        str(self.market),
            "contract_type": self.contract_type.value,
            "stake":         self.stake,
            "duration":      self.duration,
            "entry_time":    self.entry_time.isoformat(),
            "entry_trigger": self.entry_trigger,
            "signal_ids":    self.signal_ids,
            "outcome":       self.outcome,
            "payout":        self.payout,
            "exit_time":     self.exit_time.isoformat() if self.exit_time else None,
            "review":        self.review,
        }


def _review_summary(score: int, violations: list) -> str:
    if not violations:
        return f"Clean trade. Score: {score}/100."
    lines = [f"Score: {score}/100. Issues:"]
    for v in violations:
        lines.append(f"  [{v['severity'].upper()}] {v['description']}")
    return "\n".join(lines)
