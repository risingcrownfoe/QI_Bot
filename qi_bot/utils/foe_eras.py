# qi_bot/utils/foe_eras.py

ERA_ORDER = [
    "IronAge",
    "EarlyMiddleAge",
    "HighMiddleAge",
    "LateMiddleAge",
    "ColonialAge",
    "IndustrialAge",
    "ProgressiveEra",
    "ModernEra",
    "PostModernEra",
    "ContemporaryEra",
    "TomorrowEra",
    "FutureEra",
    "ArcticFuture",
    "OceanicFuture",
    "VirtualFuture",
    "SpaceAgeMars",
    "SpaceAgeAsteroidBelt",
    "SpaceAgeVenus",
    "SpaceAgeJupiterMoon",
    "SpaceAgeTitan",
    "SpaceAgeSpaceHub",
]

_ERA_INDEX = {era: i + 1 for i, era in enumerate(ERA_ORDER)}

def era_nr_from_str(era: str) -> int:
    return _ERA_INDEX.get(era, 0)

def era_str_from_nr(era_nr: int) -> str | None:
    if 1 <= era_nr <= len(ERA_ORDER):
        return ERA_ORDER[era_nr - 1]
    return None
