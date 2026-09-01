import os


SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@superset-postgres:5432/superset",
)
SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY", "local-development-secret-change-before-sharing"
)

