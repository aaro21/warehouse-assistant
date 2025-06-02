from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    STAGE_DB: str = "stage"
    BRONZE_DB: str = "bronze_db"
    SILVER_DB: str = "silver_db"
    GOLD_DB: str = "gold_db"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()

STAGE_DB = settings.STAGE_DB
BRONZE_DB = settings.BRONZE_DB
SILVER_DB = settings.SILVER_DB
GOLD_DB = settings.GOLD_DB