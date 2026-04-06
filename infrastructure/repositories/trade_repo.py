from domain.entities import Trade, Market
from domain.value_objects import ContractType
from datetime import datetime


class TradeRepository:
    def __init__(self, firebase):
        self.db = firebase

    def save(self, trade: Trade):
        self.db.save("trades", trade.id, trade.to_dict())

    def get_by_id(self, trade_id: str) -> Trade | None:
        raw = self.db.get("trades", trade_id)
        return self._from_dict(raw) if raw else None

    def get_all(self) -> list[Trade]:
        raw = self.db.query("trades", [])
        return [self._from_dict(r) for r in raw]

    def get_by_market(self, symbol: str) -> list[Trade]:
        raw = self.db.query("trades", [("market", "==", symbol)])
        return [self._from_dict(r) for r in raw]

    @staticmethod
    def _from_dict(raw: dict) -> Trade:
        trade = Trade(
            id=raw["id"],
            market=Market(symbol=raw["market"], name=raw["market"]),
            contract_type=ContractType(raw["contract_type"]),
            stake=raw["stake"],
            duration=raw["duration"],
            entry_time=datetime.fromisoformat(raw["entry_time"]),
            entry_trigger=raw["entry_trigger"],
            signal_ids=raw.get("signal_ids", []),
            outcome=raw.get("outcome"),
            payout=raw.get("payout"),
            exit_time=datetime.fromisoformat(
                raw["exit_time"]) if raw.get("exit_time") else None,
            review=raw.get("review"),
        )
        return trade
