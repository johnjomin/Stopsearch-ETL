import time
from typing import List, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .api import PoliceApiClient, ApiError


class HttpPoliceApiClient(PoliceApiClient):
    """HTTP implementation of the Police API client with retries and timeouts."""

    # Police API client that talks over HTTP, with retries and timeouts

    def __init__(self, timeout: int = 30):
        self.base_url = "https://data.police.uk/api"
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Build a requests session with retry logic"""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,  # Wait 1, 2, 4secs....
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_stops(self, force: str, year_month: str) -> List[Dict]:
        """Pull stop & search data for a specific police force and month"""
        url = f"{self.base_url}/stops-force"
        params = {
            "force": force,
            "date": year_month
        }

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            raise ApiError(f"HTTP error {response.status_code}: {e}")
        except requests.exceptions.RequestException as e:
            raise ApiError(f"Request failed: {e}")
        except ValueError as e:
            raise ApiError(f"Invalid JSON response: {e}")

    def get_available_months(self, force: str) -> List[str]:
        """Get available months with stop & search data for a force."""
        url = f"{self.base_url}/stops-force"
        params = {"force": force}

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            availability_data = response.json()

            # keep only months where this force shows up under stop-and-search
            available_months = []
            for month_data in availability_data:
                if "stop-and-search" in month_data and force in month_data["stop-and-search"]:
                    available_months.append(month_data["date"])

            return available_months

        except requests.exceptions.HTTPError as e:
            raise ApiError(f"HTTP error {response.status_code}: {e}")
        except requests.exceptions.RequestException as e:
            raise ApiError(f"Request failed: {e}")
        except ValueError as e:
            raise ApiError(f"Invalid JSON response: {e}")