"""Shared database utilities used across data-processing scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Any

import yaml
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import Engine


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"

ENV_DEFAULTS = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}


class ConfigError(Exception):
    """Raised when configuration cannot be loaded or is invalid."""


@dataclass(frozen=True)
class DatabaseConfig:
    """Simple container for database connection properties."""

    host: str
    port: int
    user: str
    password: str
    database: str

    def sqlalchemy_url(self) -> str:
        return (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "DatabaseConfig":
        missing = [key for key in ("host", "port", "user", "password", "database") if key not in data]
        if missing:
            raise ConfigError(f"Database config missing keys: {', '.join(missing)}")
        return cls(
            host=str(data["host"]),
            port=int(data["port"]),
            user=str(data["user"]),
            password=str(data["password"]),
            database=str(data["database"]),
        )

    def with_env_defaults(self) -> "DatabaseConfig":
        """Return a copy with environment variables filling in missing fields."""

        return DatabaseConfig(
            host=ENV_DEFAULTS["host"] or self.host,
            port=int(ENV_DEFAULTS["port"] or self.port),
            user=ENV_DEFAULTS["user"] or self.user,
            password=ENV_DEFAULTS["password"] or self.password,
            database=ENV_DEFAULTS["database"] or self.database,
        )


def add_config_argument(parser, default_path: Path | None = None) -> None:
    """Attach a --config option to an argparse parser."""

    default = default_path or DEFAULT_CONFIG_PATH
    parser.add_argument(
        "--config",
        default=str(default),
        help=f"Path to configuration file (default: {default})",
    )


def load_yaml_config(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, Mapping):
        raise ConfigError(f"Configuration root must be a mapping: {path}")
    return data


def load_database_config(config_path: str | Path | None = None) -> DatabaseConfig:
    """Load the database configuration from config.yaml (with env overrides)."""

    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    data = load_yaml_config(path)
    database_section = data.get("database")
    if not isinstance(database_section, Mapping):
        raise ConfigError("Config file must define a 'database' mapping.")

    defaults = {
        "host": database_section.get("host", "127.0.0.1"),
        "port": int(database_section.get("port", 3306)),
        "user": database_section.get("user", "nasdaq_user"),
        "password": database_section.get("password", "nasdaq_pass"),
        "database": database_section.get("database") or database_section.get("name") or "nasdaq",
    }
    config = DatabaseConfig.from_mapping(defaults)
    return config.with_env_defaults()


def load_database_config_from_args(args) -> DatabaseConfig:
    """Convenience wrapper to read database config using a parsed argparse namespace."""

    path = getattr(args, "config", None)
    return load_database_config(path)


def create_engine_from_config(config: DatabaseConfig) -> Engine:
    """Create a SQLAlchemy engine using the provided database config."""

    return create_engine(config.sqlalchemy_url(), future=True, pool_pre_ping=True)


def reflect_table(engine: Engine, table_name: str, metadata: MetaData | None = None) -> Table:
    """Reflect a single table definition from the database."""

    metadata = metadata or MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def reflect_tables(engine: Engine, *table_names: str, metadata: MetaData | None = None) -> Mapping[str, Table]:
    """Reflect multiple tables and return them keyed by name."""

    metadata = metadata or MetaData()
    tables: Dict[str, Table] = {}
    for name in table_names:
        tables[name] = Table(name, metadata, autoload_with=engine)
    return tables
