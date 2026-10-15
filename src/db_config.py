"""Shared ETL settings; Airflow's DB_HOST remains reserved for its entrypoint."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)


def get_db_config():
    """Use the complete FDE_DB_* namespace in Docker, otherwise local DB_*."""
    fields = {"host": "HOST", "port": "PORT", "dbname": "NAME",
              "user": "USER", "password": "PASSWORD"}
    if any(key.startswith("FDE_DB_") for key in os.environ):
        missing = ["FDE_DB_" + suffix for suffix in fields.values()
                   if not os.environ.get("FDE_DB_" + suffix)]
        if missing:
            raise ValueError("Missing ETL settings: " + ", ".join(missing))
        return {key: os.environ["FDE_DB_" + suffix]
                for key, suffix in fields.items()}
    defaults = {"HOST": "localhost", "PORT": "5432", "NAME": "fde",
                "USER": "postgres"}
    return {key: os.getenv("DB_" + suffix, defaults.get(suffix))
            for key, suffix in fields.items()}
