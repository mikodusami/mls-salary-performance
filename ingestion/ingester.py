"""
ingestion/ingester.py

abstract base class for swallowing the data for all other ingesters

handles the http stuff like fetching, rate limiting, retries, raw data persistence

all source specific ingesters inherit from this
"""

import time 
import logging
import requests
import pandas as pd

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class Ingester(ABC):
    """
    abc for all ingesters

    subclasses must implement:
    - fetch_data(): orchestates the entire thing and returns a dataframe
    - source_name(): just the identifier used in logging and file naming for organizational stuff
    
    subclasses get:
    - get(url): rate limited, retires http get w consistent headers
    - save_raw(df): saves the raw df to data/raw/source_name_raw.csv
    - structed logging via self.logger
    """

    source_name: str = 'ingester'
    default_delay: float = 4.0 # seconds between requests, we need this because of Fbref which if we dont use, we will not be able to get data from them
    max_retries: int = 3 # just so we can retry it if sh*t goes south
    backoff_factor: float = 2.0 # exponetional backoff mulitplier on retry

    def __init__(self, delay: Optional[float] = None, raw_data_dir: str = 'data/raw'):
        super().__init__()

        self.delay = delay if delay is not None else self.default_delay
        self.raw_data_dir = Path(raw_data_dir)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(self._default_headers())
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @staticmethod
    def _default_headers() -> dict:
        """
        mimic real browser so we dont get this bot detection bs from fbref
        """
        return {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    @property
    @abstractmethod
    def source_name(self) -> str:
        """short id for data source"""
        ...
    
    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        orchestrate the full scraping run for this source
        returns raw, uncleaned dataframe ready to be passed to a transformer for cleaning
        implementations call self.get(url) for every http requests so rate limiting and retries are handled consistently
        """
        ...
    
    # ------------------------------------------------------------------
    # Core HTTP helper — use this everywhere instead of requests directly
    # ------------------------------------------------------------------

    def get(self, url: str, **kwargs) -> requests.Response:
        """
        rate limited-retrying http get

        sleeps `self.delay` seconds before every request, then retries up to `self.max_retries' times w expoential backoff until failiure

        args: 
        - url: the url to fetch
        - **kwargs: passed directly to requests.Session.get()

        returns:
        - requests.Response with raise_for_status() already called

        raises:
        - httperror: if request fails after all retries
        """
        time.sleep(self.delay)
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.debug("GET %s (attempt %d/%d)", url, attempt, self.max_retries)
                response = self.session.get(url, timeout=30, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    self.logger.error("Failed to fetch %s after %d attempts: %s", url, self.max_retries, exc)
                    raise
 
                wait = self.delay * (self.backoff_factor ** (attempt - 1))
                self.logger.warning(
                    "Attempt %d/%d failed for %s — retrying in %.1fs. Error: %s",
                    attempt, self.max_retries, url, wait, exc,
                )
                time.sleep(wait)
    # ------------------------------------------------------------------
    # Persistence helper
    # ------------------------------------------------------------------
 
    def save_raw(self, df: pd.DataFrame) -> Path:
        """
        Persist the raw DataFrame to data/raw/<source_name>_raw.csv.
 
        Args:
            df: The raw scraped DataFrame.
 
        Returns:
            Path to the saved file.
        """
        if df.empty:
            self.logger.warning("save_raw called with an empty DataFrame — skipping write.")
            return
 
        out_path = self.raw_data_dir / f"{self.source_name}_raw.csv"
        df.to_csv(out_path, index=False)
        self.logger.info("Saved %d rows → %s", len(df), out_path)
        return out_path
    
    def run(self) -> pd.DataFrame:
        """
        Fetch and immediately persist raw data.
 
        This is the standard entrypoint — call scraper.run() from any
        pipeline script or notebook instead of calling fetch_data() directly.
 
        Returns:
            The raw DataFrame (same object that was saved to disk).
        """
        self.logger.info("Starting ingestion for source: %s", self.source_name)
        df = self.fetch_data()
        self.save_raw(df)
        self.logger.info("Ingestion complete for source: %s — %d rows", self.source_name, len(df))
        return df