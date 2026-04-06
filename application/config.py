from dataclasses import dataclass, field


@dataclass
class TimeframeConfig:
    candle_patterns: list[str] = field(default_factory=list)
    market_patterns: list[str] = field(default_factory=list)
    indicators:      list[str] = field(default_factory=list)


@dataclass
class AnalyzerConfig:
    candle_patterns: list[str] = field(default_factory=list)
    market_patterns: list[str] = field(default_factory=list)
    indicators:      list[str] = field(default_factory=list)


@dataclass
class SessionConfig:
    markets:    list[str]
    timeframes: TimeframeConfig
    analyzers:  AnalyzerConfig
    candle_window: int = 220
