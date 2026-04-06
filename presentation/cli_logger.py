from domain.entities import Candle, Analysis, Trade


def log_candle(symbol: str, timeframe: str, candle: Candle) -> None:
    print(
        f"  [{symbol} {timeframe}] "
        f"{candle.timestamp} | "
        f"O:{candle.open} H:{candle.high} L:{candle.low} C:{candle.close} | "
        f"Type: {candle.candle_type or 'unclassified'}"
    )


def log_analysis(analysis: Analysis) -> None:
    print(f"\n{'─'*55}")
    print(f"  ANALYSIS [{analysis.timeframe.value}] @ {analysis.timestamp}")
    print(f"  {analysis.narrative}")
    if analysis.data:
        for k, v in analysis.data.items():
            print(f"    {k}: {v}")
    print(f"{'─'*55}\n")


def log_trade(trade: Trade) -> None:
    print(f"\n{'='*55}")
    print(f"  TRADE LOGGED")
    print(f"  Market     : {trade.market}")
    print(f"  Contract   : {trade.contract_type.value}")
    print(f"  Stake      : {trade.stake}")
    print(f"  Duration   : {trade.duration}s")
    print(f"  Trigger    : {trade.entry_trigger}")
    if trade.review:
        print(f"  Score      : {trade.review['score']}/100")
    print(f"{'='*55}\n")


def log_violations(trade: Trade) -> None:
    if not trade.review:
        return
    print(f"\n  WARNING — RULE VIOLATIONS:")
    for v in trade.review["violations"]:
        print(f"    [{v['severity'].upper()}] {v['description']}")
    print()


def log_bootstrap(symbol: str, timeframe: str, count: int) -> None:
    print(f"  Bootstrapping {symbol} [{timeframe}] — {count} candles...")


def log_stream_start(symbol: str, timeframe: str) -> None:
    print(f"  Streaming {symbol} [{timeframe}] — live...")


def log_error(context: str, error: Exception) -> None:
    print(f"\n  ERROR [{context}]: {error}\n")
