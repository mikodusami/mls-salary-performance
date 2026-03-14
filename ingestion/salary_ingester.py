"""
ingestion/salary_scraper.py

Scraper for MLSPA salary data.

The MLSPA publishes salary guides as CSVs hosted on S3. The URL contains
a date-stamp that changes with each release, so there is no stable permalink.


The rest of the scraper stays unchanged.
"""

import io
import pandas as pd

from ingestion.ingester import Ingester


# -----------------------------------------------------------------------
# Update this when MLSPA publishes a new salary guide.
# -----------------------------------------------------------------------
SALARY_CSV_URL = (
    "https://s3.amazonaws.com/mlspa/MLS-Salary-List-10-2025-REVISED.csv"
    "?mtime=20251029164256"
)

# Expected columns as published by MLSPA.
# Used to validate the download before saving anything.
EXPECTED_COLUMNS = {
    "First Name",
    "Last Name",
    "Team Name",
    "Position",
    "PA Base Salary",
    "Guaranteed Comp",
}


class SalaryIngester(Ingester):
    """
    Downloads the MLSPA salary guide CSV and returns it as a raw DataFrame.

    No cleaning happens here — that lives in transform/salary_cleaner.py.
    This class is only responsible for getting the bytes off the wire and
    turning them into a DataFrame.
    """

    source_name = "salaries"

    # The MLSPA CSV is a single lightweight file, not a paginated site.
    # We only make one request, so rate limiting isn't a concern here.
    # Set delay to 0 to avoid an unnecessary sleep on a single-shot download.
    default_delay = 0.0

    def __init__(self, url: str = SALARY_CSV_URL, **kwargs):
        """
        Args:
            url:    Direct URL to the MLSPA salary CSV. Defaults to the
                    constant above. Pass a different URL if you want to
                    target a different release year without touching the file.
            **kwargs: Forwarded to Ingester (e.g. raw_data_dir).
        """
        super().__init__(**kwargs)
        self.url = url

    def fetch_data(self) -> pd.DataFrame:
        """
        Download the MLSPA salary CSV and return it as a raw DataFrame.

        The CSV is downloaded into memory (no temp file) and parsed directly
        by pandas. Column names are stripped of leading/trailing whitespace
        because MLSPA has historically included padding in their headers.

        Returns:
            Raw DataFrame with one row per player. No cleaning applied.

        Raises:
            ValueError: if the downloaded file is missing expected columns,
                        which likely means the URL is stale or the MLSPA
                        changed their format.
            requests.HTTPError: if the download fails (propagated from BaseScraper.get).
        """
        self.logger.info("Downloading MLSPA salary CSV from: %s", self.url)
        response = self.get(self.url)

        df = pd.read_csv(io.StringIO(response.text))

        # Strip whitespace from column names — MLSPA CSVs have had padding before
        df.columns = df.columns.str.strip()

        self._validate_columns(df)

        self.logger.info(
            "Downloaded salary data: %d players across %d clubs",
            len(df),
            df["Team Name"].nunique() if "Team Name" in df.columns else "?",
        )

        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_columns(self, df: pd.DataFrame) -> None:
        """
        Confirm the downloaded CSV has the columns we expect.

        MLSPA occasionally reformats their release. If this raises, the URL
        is either stale or the schema changed — inspect the raw file and
        update EXPECTED_COLUMNS accordingly.

        Args:
            df: The freshly parsed DataFrame.

        Raises:
            ValueError: listing whichever expected columns are missing.
        """
        missing = EXPECTED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(
                f"MLSPA salary CSV is missing expected columns: {missing}. "
                f"Actual columns: {list(df.columns)}. "
                f"The URL may be stale or the MLSPA changed their format."
            )