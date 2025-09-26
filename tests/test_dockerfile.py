import pytest
import subprocess
import os


def test_dockerfile_exists():
    # Arrange & Act
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Assert
    assert os.path.exists(dockerfile_path), "Dockerfile should exist in project root"


def test_dockerfile_uses_python_slim():
    # Arrange
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Act
    with open(dockerfile_path, 'r') as f:
        content = f.read()

    # Assert
    assert 'python:3.13-slim' in content.lower(), "Should use Python slim image for smaller size"


def test_dockerfile_creates_nonroot_user():
    # Arrange
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Act
    with open(dockerfile_path, 'r') as f:
        content = f.read()

    # Assert
    assert ('adduser' in content or 'useradd' in content), "Should create non-root user"
    assert 'USER' in content, "Should switch to non-root user"


def test_dockerfile_has_healthcheck():
    # Arrange
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Act
    with open(dockerfile_path, 'r') as f:
        content = f.read()

    # Assert
    assert 'HEALTHCHECK' in content, "Should include healthcheck instruction"


def test_dockerfile_sets_working_directory():
    # Arrange
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Act
    with open(dockerfile_path, 'r') as f:
        content = f.read()

    # Assert
    assert 'WORKDIR' in content, "Should set working directory"


def test_dockerfile_installs_dependencies():
    # Arrange
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Act
    with open(dockerfile_path, 'r') as f:
        content = f.read()

    # Assert
    assert 'pip install' in content, "Should install Python dependencies"


def test_dockerfile_has_proper_entrypoint():
    # Arrange
    dockerfile_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Dockerfile')

    # Act
    with open(dockerfile_path, 'r') as f:
        content = f.read()

    # Assert
    assert 'ENTRYPOINT' in content or 'CMD' in content, "Should have proper entrypoint"


def test_requirements_file_exists(): # requirements must exist for build
    # Arrange
    requirements_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'requirements.txt')

    # Act & Assert
    assert os.path.exists(requirements_path), "requirements.txt should exist for Docker build"


@pytest.mark.slow
def test_docker_build_succeeds(): # integration: try to build the image
    # Arrange
    project_root = os.path.dirname(os.path.dirname(__file__))

    # Act
    try:
        result = subprocess.run(
            ['docker', 'build', '-t', 'stopsearch-etl-test', '.'],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes
        )

        # Assert
        assert result.returncode == 0, f"Docker build failed: {result.stderr}"

    except subprocess.TimeoutExpired:
        pytest.skip("Docker build timed out - skipping integration test")
    except FileNotFoundError:
        pytest.skip("Docker not available - skipping integration test")