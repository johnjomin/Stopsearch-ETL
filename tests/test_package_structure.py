import importlib


def test_can_import_main_package():
    # Arrange
    package_name = "stopsearch_etl"

    # Act
    try:
        module = importlib.import_module(package_name)
        import_successful = True
    except ImportError:
        import_successful = False

    # Assert
    assert import_successful, f"Should be able to import {package_name}"