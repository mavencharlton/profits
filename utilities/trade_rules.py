from domain.entities import Trade


def no_trade_against_trend(trade: Trade) -> dict | None:
    trend = trade.review and trade.review.get("context", {}).get("trend")
    if trend == "bullish" and trade.contract_type.value == "FALL":
        return {"rule_id": "no_trade_against_trend",
                "description": "Selling in a confirmed bullish trend.",
                "severity": "high"}
    if trend == "bearish" and trade.contract_type.value == "RISE":
        return {"rule_id": "no_trade_against_trend",
                "description": "Buying in a confirmed bearish trend.",
                "severity": "high"}
    return None


def no_trade_during_squeeze(trade: Trade) -> dict | None:
    squeeze = trade.review and trade.review.get("context", {}).get("squeeze")
    if squeeze:
        return {"rule_id": "no_trade_during_squeeze",
                "description": "Entering while squeeze is active — low momentum.",
                "severity": "medium"}
    return None


def require_entry_trigger(trade: Trade) -> dict | None:
    if not trade.entry_trigger or trade.entry_trigger == "manual":
        return {"rule_id": "require_entry_trigger",
                "description": "No clear entry trigger recorded.",
                "severity": "low"}
    return None


def get_rules() -> list:
    return [
        no_trade_against_trend,
        no_trade_during_squeeze,
        require_entry_trigger,
    ]
