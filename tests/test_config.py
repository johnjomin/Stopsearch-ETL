import os
import pytest
from stopsearch_etl.config import Config


def test_config_loads_defaults():
    # Arrange
    # clear env to test defaults
    old_env = {}
    for key in ["FORCES", "DATABASE_URL", "LOG_LEVEL"]:
        old_env[key] = os.environ.pop(key, None)

    try:
        # Act
        config = Config()

        # Assert
        assert config.forces == ["metropolitan"]  # sensible default
        assert config.database_url == "sqlite:///stopsearch.db"
        assert config.log_level == "INFO"

    finally:
        # put env bac
        for key, value in old_env.items():
            if value is not None:
                os.environ[key] = value


def test_config_loads_from_environment():
    # Arrange
    os.environ["FORCES"] = "metropolitan,avon-and-somerset"
    os.environ["DATABASE_URL"] = "postgresql://user:pass@localhost/db"
    os.environ["LOG_LEVEL"] = "DEBUG"

    try:
        # Act
        config = Config()

        # Assert
        assert config.forces == ["metropolitan", "avon-and-somerset"]
        assert config.database_url == "postgresql://user:pass@localhost/db"
        assert config.log_level == "DEBUG"

    finally:
        # cleanup
        for key in ["FORCES", "DATABASE_URL", "LOG_LEVEL"]:
            os.environ.pop(key, None)


def test_config_validates_log_level():
    # Arrange
    os.environ["LOG_LEVEL"] = "INVALID"

    try:
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            Config()

        assert "Invalid log level" in str(exc_info.value)

    finally:
        # cleanup
        os.environ.pop("LOG_LEVEL", None)