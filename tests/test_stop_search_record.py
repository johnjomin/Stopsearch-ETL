from datetime import datetime
import pytest
from stopsearch_etl.domain import StopSearchRecord


def test_stop_search_record_creation_with_valid_data():
    # Arrange
    # pretend the API gave us a chunky JSON dict
    record_data = {
        "type": "Person search",
        "datetime": "2023-01-15T14:30:00+00:00",
        "gender": "Male",
        "age_range": "25-34",
        "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
        "officer_defined_ethnicity": "White",
        "legislation": "Police and Criminal Evidence Act 1984 (section 1)",
        "object_of_search": "Drugs",
        "outcome": "A no further action disposal",
        "outcome_linked_to_object_of_search": False,
        "removal_of_more_than_outer_clothing": False,
        "location": {
            "latitude": 51.5074,
            "longitude": -0.1278,
            "street": {
                "id": 883407,
                "name": "On or near High Street"
            }
        }
    }

    # Act: feed the JSON into our class method
    record = StopSearchRecord.from_api_data(record_data)

    # Assert
    # check the important bits actually survived
    assert record.type == "Person search"
    assert record.gender == "Male"
    assert record.age_range == "25-34"
    assert record.outcome == "A no further action disposal"
    assert record.latitude == 51.5074
    assert record.longitude == -0.1278

def test_stop_search_record_handles_missing_fields():
    # Arrange: deliberately striping out some fields :)
    record_data = {"type": "Vehicle search", "datetime": "2023-02-01T00:00:00Z"}

    # Act
    record = StopSearchRecord.from_api_data(record_data)

    # Assert: should still produce a record without blowing up (finger crossed)
    assert record.type == "Vehicle search"
    assert isinstance(record.datetime, datetime)

    # gender wasn’t supplied
    assert record.gender is None

    # legislation falls back to empty string
    assert record.legislation == ""