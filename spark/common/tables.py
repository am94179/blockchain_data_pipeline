from common.settings import ICEBERG_CATALOG


def table(stage: str, entity: str) -> str:
    """Return a fully qualified Iceberg table name."""
    return f"{ICEBERG_CATALOG}.{stage}.{entity}"


def bronze(entity: str) -> str:
    return table("bronze", entity)


def silver(entity: str) -> str:
    return table("silver", entity)


def silver_rejects(entity: str) -> str:
    return table("silver_rejects", entity)


def gold(entity: str) -> str:
    return table("gold", entity)
