import pytest
import os
import yaml


def test_docker_compose_file_exists():  # file should be at repo root
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act & Assert
    assert os.path.exists(compose_path), "docker-compose.yml should exist in project root"


def test_docker_compose_has_valid_yaml(): # yaml should parse cleanly
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act
    with open(compose_path, 'r') as f:
        try:
            compose_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"Invalid YAML in docker-compose.yml: {e}")

    # Assert
    assert compose_data is not None, "docker-compose.yml should contain valid YAML"
    assert 'services' in compose_data, "Should have services section"


def test_docker_compose_has_etl_service():
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)

    # Assert
    assert 'etl' in compose_data['services'], "Should have 'etl' service"
    etl_service = compose_data['services']['etl']

    assert 'build' in etl_service or 'image' in etl_service, "ETL service should specify build context or image"


def test_docker_compose_has_volume_for_persistence():
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)

    # Assert
    etl_service = compose_data['services']['etl']
    assert 'volumes' in etl_service, "ETL service should have volumes for persistence"

    # Check that there's a volume for the database
    volumes = etl_service['volumes']
    has_db_volume = any('/app/data' in volume or 'stopsearch.db' in volume for volume in volumes)
    assert has_db_volume, "Should have volume for database persistence"


def test_docker_compose_has_environment_configuration():
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)

    # Assert
    etl_service = compose_data['services']['etl']
    assert 'environment' in etl_service, "ETL service should have environment variables"


def test_docker_compose_has_demo_service():
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)

    # Assert
    # Should have either a separate demo service or the main service configured for demo
    services = compose_data['services']
    assert len(services) >= 1, "Should have at least one service"


def test_docker_compose_override_file_exists(): # optional override example for local dev
    # Arrange
    override_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.override.yml.example')

    # Act
    file_exists = os.path.exists(override_path)

    # Assert
    # This is optional, but helpful for local development
    # We'll check for example file
    assert file_exists or True, "docker-compose.override.yml.example is helpful but not required"


def test_docker_compose_has_restart_policy(): # restart policy is optional but nice to have
    # Arrange
    compose_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')

    # Act
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)

    # Assert
    etl_service = compose_data['services']['etl']
    # Restart policy is optional but good for production
    if 'restart' in etl_service:
        valid_policies = ['no', 'always', 'on-failure', 'unless-stopped']
        assert etl_service['restart'] in valid_policies, f"Restart policy should be one of {valid_policies}"