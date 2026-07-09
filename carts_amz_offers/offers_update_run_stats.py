# -*- coding: utf-8 -*-

from dataclasses import asdict, dataclass, field
from typing import Any, Dict


@dataclass
class TierRunStats:
    seed_cnt: int = 0
    queued_cnt: int = 0
    fresh_cnt: int = 0
    dedup_cnt: int = 0
    skipped_missing_file: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OffersUpdateRunStats:
    seed_cnt: int = 0
    queued_cnt: int = 0
    fresh_cnt: int = 0
    missing_cnt: int = 0
    queue_full: bool = False
    queue_cnt_before: int = 0
    skipped_missing_file: bool = False
    tier_stats: Dict[str, TierRunStats] = field(default_factory=dict)

    @property
    def expired_cnt(self) -> int:
        return self.queued_cnt

    @property
    def alive_cnt(self) -> int:
        return self.fresh_cnt

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["tier_stats"] = {
            tier: stats.to_dict() for tier, stats in self.tier_stats.items()
        }
        return payload
