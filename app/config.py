import os
from sqlalchemy.pool import NullPool

# Absolute path to instance folder (same directory as this file's parent)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB = f"sqlite:///{os.path.join(_BASE_DIR, 'instance', 'socialpulse.db')}"


def _sqlite_engine_options(uri):
    """Return SQLAlchemy engine options tuned for SQLite concurrency."""
    if uri and uri.startswith("sqlite"):
        return {
            "connect_args": {"timeout": 30},
            "pool_pre_ping": True,
            "poolclass": NullPool,
        }
    return {}


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", _DEFAULT_DB)
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = _sqlite_engine_options(
        os.getenv("DATABASE_URL", _DEFAULT_DB)
    )
    WTF_CSRF_ENABLED = True

    # Mail settings for email verification
    MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.gmail.com")
    MAIL_PORT = int(os.getenv("MAIL_PORT", 587))
    MAIL_USE_TLS = os.getenv("MAIL_USE_TLS", "true").lower() == "true"
    MAIL_USE_SSL = os.getenv("MAIL_USE_SSL", "false").lower() == "true"
    MAIL_USERNAME = os.getenv("MAIL_USERNAME", "nithya@fourdm.digital")
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")
    MAIL_DEFAULT_SENDER = os.getenv("MAIL_DEFAULT_SENDER", "nithya@fourdm.digital")


class DevelopmentConfig(Config):
    DEBUG = True
    PREFERRED_URL_SCHEME = "https"


class ProductionConfig(Config):
    DEBUG = False


config_map = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
}
