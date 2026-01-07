"""
Configuration Management for EMS ETL Pipeline
Author: J. Ryan Butcher

Handles environment-specific configuration loading and validation.
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    db_type: str = "sqlite"
    sqlite_path: str = "./db/ems-warehouse.db"
    sqlserver_server: Optional[str] = None
    sqlserver_database: Optional[str] = None
    sqlserver_trusted: bool = True


@dataclass
class ETLConfig:
    """ETL processing configuration."""
    batch_size: int = 50000
    load_type: str = "full"  # full or incremental
    source_path: str = "./data/input/"
    archive_path: str = "./data/archive/"
    rejected_path: str = "./data/rejected/"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    log_file: str = "./logs/etl.log"
    console_output: bool = True


@dataclass
class Config:
    """Main configuration container."""
    environment: str = "dev"
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    etl: ETLConfig = field(default_factory=ETLConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_yaml(cls, config_path: str, env: str = "dev") -> "Config":
        """
        Load configuration from YAML file with environment override.

        Args:
            config_path: Path to base config.yaml
            env: Environment name (dev, test, prod)

        Returns:
            Config instance with merged settings
        """
        base_path = Path(config_path)

        # Load base config
        config_data = {}
        if base_path.exists():
            with open(base_path, "r") as f:
                config_data = yaml.safe_load(f) or {}

        # Load environment-specific override
        env_path = base_path.parent / f"config.{env}.yaml"
        if env_path.exists():
            with open(env_path, "r") as f:
                env_data = yaml.safe_load(f) or {}
                config_data = deep_merge(config_data, env_data)

        # Build config objects
        db_data = config_data.get("database", {})
        etl_data = config_data.get("etl", {})
        log_data = config_data.get("logging", {})

        return cls(
            environment=env,
            database=DatabaseConfig(
                db_type=db_data.get("type", "sqlite"),
                sqlite_path=db_data.get("sqlite_path", "./db/ems-warehouse.db"),
                sqlserver_server=db_data.get("sqlserver", {}).get("server"),
                sqlserver_database=db_data.get("sqlserver", {}).get("database"),
                sqlserver_trusted=db_data.get("sqlserver", {}).get("trusted_connection", True),
            ),
            etl=ETLConfig(
                batch_size=etl_data.get("batch_size", 50000),
                load_type=etl_data.get("load_type", "full"),
                source_path=etl_data.get("source_path", "./data/input/"),
                archive_path=etl_data.get("archive_path", "./data/archive/"),
                rejected_path=etl_data.get("rejected_path", "./data/rejected/"),
            ),
            logging=LoggingConfig(
                level=log_data.get("level", "INFO"),
                log_file=log_data.get("log_file", "./logs/etl.log"),
                console_output=log_data.get("console_output", True),
            ),
        )

    def get_db_connection_string(self) -> str:
        """Get database connection string based on config."""
        if self.database.db_type == "sqlite":
            return self.database.sqlite_path
        else:
            # SQL Server connection string
            server = self.database.sqlserver_server
            db = self.database.sqlserver_database
            if self.database.sqlserver_trusted:
                return f"mssql+pyodbc://{server}/{db}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            return f"mssql+pyodbc://{server}/{db}?driver=ODBC+Driver+17+for+SQL+Server"


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


# Default config instance
_config: Optional[Config] = None


def get_config(config_path: str = None, env: str = None) -> Config:
    """
    Get or create configuration singleton.

    Args:
        config_path: Path to config file (uses default if None)
        env: Environment override (uses ENV var or 'dev' if None)

    Returns:
        Config instance
    """
    global _config

    if _config is None or config_path is not None:
        if config_path is None:
            # Find config relative to this file
            base_dir = Path(__file__).parent.parent.parent
            config_path = str(base_dir / "config" / "config.yaml")

        if env is None:
            env = os.environ.get("ETL_ENV", "dev")

        _config = Config.from_yaml(config_path, env)

    return _config


if __name__ == "__main__":
    # Test configuration loading
    config = get_config()
    print(f"Environment: {config.environment}")
    print(f"Database type: {config.database.db_type}")
    print(f"Batch size: {config.etl.batch_size}")
    print(f"Log level: {config.logging.level}")
