from __future__ import annotations

import re


NON_PLACE_KEYWORDS = (
    "包车",
    "一日游",
    "半日游",
    "休整",
    "自由活动",
    "自由行",
    "返程",
    "回程",
    "返回",
    "大巴",
    "火车软卧",
    "软卧",
    "硬卧",
    "高铁",
    "动车",
    "航班",
    "飞往",
    "抵达",
    "出发",
    "接机",
    "送机",
    "深潜",
    "浮潜",
    "体验潜水",
    "vip",
    "VIP",
    "篝火",
    "星空",
)

PLACE_HINT_KEYWORDS = (
    "hotel",
    "hostel",
    "guest house",
    "guesthouse",
    "inn",
    "resort",
    "museum",
    "temple",
    "church",
    "market",
    "airport",
    "station",
    "beach",
    "island",
    "lake",
    "castle",
    "fort",
    "plaza",
    "mall",
)


def should_skip_place_query(query: str, category: str | None = None) -> bool:
    compact = str(query or "").strip()
    if not compact:
        return True

    lowered = compact.lower()
    normalized = re.sub(r"\s+", "", lowered)

    if category in {"hotel", "restaurant", "attraction", "shopping"}:
        return False

    for keyword in PLACE_HINT_KEYWORDS:
        if keyword in lowered:
            return False

    for keyword in NON_PLACE_KEYWORDS:
        if keyword.lower() in normalized:
            return True

    if len(normalized) <= 2 and not re.search(r"[\u4e00-\u9fffA-Za-z]", normalized):
        return True

    return False
