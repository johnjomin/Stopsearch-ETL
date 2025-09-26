import time
from typing import List, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .api import PoliceApiClient, ApiError


class HttpPoliceApiClient(PoliceApiClient):
    """HTTP client with retries and timeouts"""

    # TODO: set a custom User-Agent so API owners can contact you if needed

    # Police API client that talks over HTTP, with retries and timeouts

    def __init__(self, timeout: int = 30, max_retries: int = 3, backoff_factor: float = 1.0):
        self.base_url = "https://data.police.uk/api"
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Build a requests session with retry logic"""
        session = requests.Session()

        # Configure retry strategy with configurable parameters
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        # NOTE: urllib3 may warn about allowed_methods type in older versions

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_stops(self, force: str, year_month: str) -> List[Dict]:
        """Get stop & search data for one force and month"""
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
        """List months that have stop & search for this force"""
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
            # NOTE: same as above; consider e.response.status_code
            raise ApiError(f"HTTP error {response.status_code}: {e}")
        except requests.exceptions.RequestException as e:
            raise ApiError(f"Request failed: {e}")
        except ValueError as e:
            raise ApiError(f"Invalid JSON response: {e}")