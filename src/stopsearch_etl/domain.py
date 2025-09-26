from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StopSearchRecord:
    """A stop and search record from UK Police data."""
    # keeping it simple: one row in the DB = one police stop/search record
    type: str
    datetime: datetime
    gender: Optional[str]
    age_range: Optional[str]
    self_defined_ethnicity: Optional[str]
    officer_defined_ethnicity: Optional[str]
    legislation: str
    object_of_search: Optional[str]
    outcome: Optional[str]
    outcome_linked_to_object_of_search: Optional[bool]
    removal_of_more_than_outer_clothing: Optional[bool]
    latitude: Optional[float]
    longitude: Optional[float]
    street_id: Optional[int]
    street_name: Optional[str]

    @classmethod
    def from_api_data(cls, data: dict) -> "StopSearchRecord":
        """Create a StopSearchRecord from Police API JSON data."""

        # datetime comes as a string like "2023-01-15T14:30:00+00:00"
        dt = datetime.fromisoformat(data["datetime"].replace("Z", "+00:00"))

        # location might be missing or half-empty
        location = data.get("location", {})
        street = location.get("street", {})

        return cls(
            type=data.get("type", "Unknown"),
            datetime=dt,
            gender=data.get("gender"),
            age_range=data.get("age_range"),
            self_defined_ethnicity=data.get("self_defined_ethnicity"),
            officer_defined_ethnicity=data.get("officer_defined_ethnicity"),
            legislation=data.get("legislation", ""),  # at least empty string if missing
            object_of_search=data.get("object_of_search"),
            outcome=data.get("outcome"),
            outcome_linked_to_object_of_search=data.get("outcome_linked_to_object_of_search"),
            removal_of_more_than_outer_clothing=data.get("removal_of_more_than_outer_clothing"),
            latitude=location.get("latitude"),
            longitude=location.get("longitude"),
            street_id=street.get("id"),
            street_name=street.get("name"),
        )