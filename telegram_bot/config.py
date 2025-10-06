import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
AUTHORIZED_USERS = os.getenv("TELEGRAM_AUTHORIZED_USERS", "").split(",")

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")

TRINO_HOST = os.getenv("TRINO_HOST")
TRINO_PORT = int(os.getenv("TRINO_PORT"))
TRINO_USER = os.getenv("TRINO_USER")
TRINO_CATALOG = os.getenv("TRINO_CATALOG")

SUPERSET_URL = os.getenv("SUPERSET_URL")
SUPERSET_PUBLIC_URL = os.getenv("SUPERSET_PUBLIC_URL")
SUPERSET_USER = os.getenv("SUPERSET_USER")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN must be set in .env file")