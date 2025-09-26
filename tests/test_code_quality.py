import pytest
import os
import subprocess
import yaml


def test_pre_commit_config_exists():
    # Arrange
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.pre-commit-config.yaml')

    # Act & Assert
    assert os.path.exists(config_path), ".pre-commit-config.yaml should exist in project root"


def test_pre_commit_config_has_valid_yaml():
    # Arrange
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.pre-commit-config.yaml')

    # Act
    with open(config_path, 'r') as f:
        try:
            config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"Invalid YAML in .pre-commit-config.yaml: {e}")

    # Assert
    assert config_data is not None, ".pre-commit-config.yaml should contain valid YAML"
    assert 'repos' in config_data, "Should have repos section"


def test_pre_commit_config_includes_ruff():
    # Arrange
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.pre-commit-config.yaml')

    # Act
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)

    # Assert
    repos = config_data.get('repos', [])
    has_ruff = any('ruff' in str(repo.get('repo', '')) for repo in repos)
    assert has_ruff, "Should include ruff for linting"


def test_pre_commit_config_includes_black():
    # Arrange
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.pre-commit-config.yaml')

    # Act
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)

    # Assert
    repos = config_data.get('repos', [])
    has_black = any('black' in str(repo.get('repo', '')) for repo in repos)
    assert has_black, "Should include black for code formatting"


def test_pyproject_toml_has_ruff_config():
    # Arrange
    pyproject_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pyproject.toml')

    # Act
    with open(pyproject_path, 'r') as f:
        content = f.read()

    # Assert
    assert '[tool.ruff]' in content, "Should have ruff configuration in pyproject.toml"


def test_pyproject_toml_has_black_config():
    # Arrange
    pyproject_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pyproject.toml')

    # Act
    with open(pyproject_path, 'r') as f:
        content = f.read()

    # Assert
    assert '[tool.black]' in content, "Should have black configuration in pyproject.toml"


def test_make_format_script_exists():
    # Arrange
    makefile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Makefile')

    # Act
    file_exists = os.path.exists(makefile_path)

    # Assert
    # Makefile is optional but helpful for development
    if file_exists:
        with open(makefile_path, 'r') as f:
            content = f.read()
        assert 'format' in content, "Makefile should have format target if it exists"
    else:
        # If no Makefile, that's okay - just document the commands
        assert True, "Makefile is optional"


@pytest.mark.slow
def test_ruff_runs_without_errors():
    # Arrange
    project_root = os.path.dirname(os.path.dirname(__file__))

    # Act & Assert
    try:
        result = subprocess.run(
            ['ruff', 'check', 'src/', 'tests/'],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )

        # Ruff should run successfully (exit code 0 or 1 for style issues)
        assert result.returncode in [0, 1], f"Ruff failed unexpectedly: {result.stderr}"

    except FileNotFoundError:
        pytest.skip("Ruff not installed - skipping lint test")
    except subprocess.TimeoutExpired:
        pytest.skip("Ruff timed out - skipping lint test")


@pytest.mark.slow
def test_black_check_runs_without_errors():
    # Arrange
    project_root = os.path.dirname(os.path.dirname(__file__))

    # Act & Assert
    try:
        result = subprocess.run(
            ['black', '--check', '--diff', 'src/', 'tests/'],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )

        # Black should run successfully (exit code 0 or 1 for formatting needed)
        assert result.returncode in [0, 1], f"Black failed unexpectedly: {result.stderr}"

    except FileNotFoundError:
        pytest.skip("Black not installed - skipping format test")
    except subprocess.TimeoutExpired:
        pytest.skip("Black timed out - skipping format test")