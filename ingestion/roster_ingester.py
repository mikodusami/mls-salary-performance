"""
ingestion/roster_ingester.py

Scraper for MLS team rosters and player profiles from mlssoccer.com.

mlssoccer.com is JavaScript-rendered — requests alone cannot fetch it.
This scraper wraps Playwright inside fetch_data(), keeping it as a private
implementation detail. Everything else (save, run, logging, raw persistence)
is inherited from Ingester unchanged.

Architecture:
    Ingester.run()
        └── fetch_data()                   ← orchestrates the full scrape
              ├── _discover_teams()         ← finds all 30 club roster URLs
              ├── _scrape_team_roster()     ← parses each team's roster table
              └── _fetch_player_profile()   ← visits each player's profile page

Playwright is only ever called through _browser_get(), which mirrors the
Ingester.get() interface: rate-limited, retrying, and logged consistently.

Installation:
    pip install playwright
    playwright install chromium
"""

import re
import time
import pandas as pd

from typing import Any, Optional
from bs4 import BeautifulSoup

from ingestion.ingester import Ingester


BASE_URL = "https://www.mlssoccer.com"
PLAYERS_URL = f"{BASE_URL}/players/"

# Seconds to wait between Playwright navigations.
# mlssoccer.com is more sensitive to rapid requests than a static S3 file.
# 3 seconds is conservative but respectful given ~800 profile page visits.
NAVIGATION_DELAY = 3.0

# Playwright will wait up to this many ms for the page to be ready.
PLAYWRIGHT_TIMEOUT_MS = 60_000


class RosterIngester(Ingester):
    """
    Scrapes MLS team rosters and player profiles from mlssoccer.com.

    Flow:
        1. Visit /players/ → discover all 30 team roster URLs
        2. For each team → parse roster table rows
        3. For each player → visit profile page and extract all available fields
        4. Return a flat DataFrame with one row per player

    All Playwright usage is confined to _browser_get(). Nothing outside
    that method touches the browser directly.
    """

    source_name = "rosters"

    # Ingester.get() is not used here — Playwright handles navigation.
    # Set delay to 0 so the inherited get() doesn't sleep if accidentally called.
    default_delay = 0.0

    def __init__(
        self,
        headless: bool = True,
        navigation_delay: float = NAVIGATION_DELAY,
        **kwargs,
    ):
        """
        Args:
            headless:          Run Chromium headlessly. Set False to debug visually.
            navigation_delay:  Seconds to sleep between every Playwright navigation.
            **kwargs:          Forwarded to Ingester (e.g. raw_data_dir).
        """
        super().__init__(**kwargs)
        self.headless = headless
        self.navigation_delay = navigation_delay

        # Playwright objects — initialised lazily inside fetch_data()
        # so importing this module never requires a running browser.
        self._playwright = None
        self._browser = None
        self._page = None

    # ------------------------------------------------------------------
    # Ingester interface
    # ------------------------------------------------------------------

    def fetch_data(self) -> pd.DataFrame:
        """
        Run the full roster scrape and return a raw DataFrame.

        Opens a single Playwright browser for the entire run (one browser,
        one page, reused across all navigations) then closes it cleanly
        even if an exception is raised mid-scrape.

        Returns:
            Raw DataFrame with one row per player. No cleaning applied.
        """
        try:
            self._start_browser()
            teams = self._discover_teams()
            players = []

            for team in teams:
                team_players = self._scrape_team_roster(team)
                players.extend(team_players)
                self.logger.info(
                    "  Scraped %d players from %s (running total: %d)",
                    len(team_players), team["name"], len(players),
                )

            self.logger.info("Total players scraped: %d", len(players))
            return pd.DataFrame(players)

        finally:
            self._stop_browser()

    # ------------------------------------------------------------------
    # Browser lifecycle — called only from fetch_data()
    # ------------------------------------------------------------------

    def _start_browser(self) -> None:
        """Launch Playwright and open a single reusable page."""
        from playwright.sync_api import sync_playwright  # local import — keeps top-level import optional

        self.logger.info("Starting Playwright browser (headless=%s)", self.headless)
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        self._page = self._browser.new_page()
        self._page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)

        # Mimic a real browser — reuse the same headers as Ingester
        self._page.set_extra_http_headers(self._default_headers())

    def _stop_browser(self) -> None:
        """Close browser and Playwright cleanly."""
        try:
            if self._browser:
                self._browser.close()
            if self._playwright:
                self._playwright.stop()
        except Exception as exc:
            self.logger.warning("Error during browser shutdown: %s", exc)
        finally:
            self._playwright = None
            self._browser = None
            self._page = None

    # ------------------------------------------------------------------
    # Core navigation primitive — the Playwright equivalent of Ingester.get()
    # ------------------------------------------------------------------

    def _browser_get(self, url: str, wait_for: str = "networkidle") -> str:
        """
        Navigate to a URL with Playwright and return the fully-rendered HTML.

        This is the single point of contact between the scraper logic and
        the browser. Rate limiting and retries live here, mirroring the
        contract of Ingester.get().

        Args:
            url:       The page URL to load.
            wait_for:  Playwright wait_until strategy. "networkidle" waits for
                       all network requests to settle — good for JS-heavy pages.
                       Use "domcontentloaded" if a page hangs on networkidle.

        Returns:
            Fully rendered HTML as a string.

        Raises:
            Exception: if the navigation fails after max_retries attempts.
        """
        time.sleep(self.navigation_delay)

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.debug("Navigating to %s (attempt %d/%d)", url, attempt, self.max_retries)
                self._page.goto(url, wait_until=wait_for)
                return self._page.content()

            except Exception as exc:
                if attempt == self.max_retries:
                    self.logger.error(
                        "Failed to navigate to %s after %d attempts: %s",
                        url, self.max_retries, exc,
                    )
                    raise

                wait = self.navigation_delay * (self.backoff_factor ** (attempt - 1))
                self.logger.warning(
                    "Attempt %d/%d failed for %s — retrying in %.1fs. Error: %s",
                    attempt, self.max_retries, url, wait, exc,
                )
                time.sleep(wait)

    # ------------------------------------------------------------------
    # Scraping logic
    # ------------------------------------------------------------------

    def _discover_teams(self) -> list[dict]:
        """
        Visit /players/ and extract all 30 team roster URLs.

        Returns:
            List of dicts with keys: name, slug, roster_url
        """
        self.logger.info("Discovering teams from %s", PLAYERS_URL)
        html = self._browser_get(PLAYERS_URL)
        soup = BeautifulSoup(html, "html.parser")

        seen_slugs = set()
        teams = []

        for link in soup.find_all("a", href=lambda h: h and "/roster" in h):
            match = re.search(r"/clubs/([^/]+)/roster", link["href"])
            if not match:
                continue
            slug = match.group(1)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            teams.append({
                "name": slug.replace("-", " ").title(),
                "slug": slug,
                "roster_url": f"{BASE_URL}/clubs/{slug}/roster/",
            })

        self.logger.info("Discovered %d teams", len(teams))
        return teams

    def _scrape_team_roster(self, team: dict) -> list[dict]:
        """
        Parse a team's roster page and fetch every player's profile.

        Args:
            team: Dict from _discover_teams() with name, slug, roster_url.

        Returns:
            List of player dicts for this team.
        """
        html = self._browser_get(team["roster_url"])
        soup = BeautifulSoup(html, "html.parser")

        tables = soup.find_all("table")
        if not tables:
            self.logger.warning("No roster tables found for %s", team["name"])
            return []

        seen_urls = set()
        team_players = []

        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers or "player" not in headers:
                continue

            for row in table.find_all("tr")[1:]:  # skip header
                cells = row.find_all("td")
                if not cells:
                    continue

                player = self._parse_roster_row(row, cells, headers, team)
                if not player:
                    continue

                url = player.get("player_url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                self._fetch_player_profile(player)
                team_players.append(player)

        return team_players

    def _parse_roster_row(
        self,
        row,
        cells: list,
        headers: list[str],
        team: dict,
    ) -> Optional[dict[str, Any]]:
        """
        Extract structured data from a single roster table row.

        Expected headers (may vary):
            player | jersey # | position | roster category | player category | player status

        Args:
            row:     The <tr> BeautifulSoup element.
            cells:   Pre-extracted <td> elements from the row.
            headers: Lowercased header strings for column mapping.
            team:    Team dict from _discover_teams().

        Returns:
            Player dict or None if the row could not be parsed.
        """
        try:
            player_link = row.select_one("a.mls-o-table__href, a[href*='/players/']")
            if not player_link:
                return None

            href = player_link.get("href", "")
            player_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Player name
            name_elem = player_link.select_one(".short-name, .mls-o-table__player-name")
            player_name = (
                name_elem.get_text(strip=True)
                if name_elem
                else player_link.get_text(strip=True)
            )

            # Thumbnail from roster table (low-res; profile page has the full image)
            player_thumb = None
            img = player_link.select_one("img")
            if img:
                player_thumb = img.get("src")

            cell_texts = [c.get_text(strip=True) for c in cells]

            player: dict[str, Any] = {
                "team_name": team["name"],
                "team_slug": team["slug"],
                "player_name": player_name,
                "player_url": player_url,
                "player_image_thumb": player_thumb,
            }

            # Map table columns to field names
            column_map = {
                "jersey #":       "jersey_number",
                "position":       "position",
                "roster category": "roster_category",
                "player category": "player_category",
                "player status":  "player_status",
            }
            for i, header in enumerate(headers):
                if header in column_map and i < len(cell_texts):
                    player[column_map[header]] = cell_texts[i] or None

            return player

        except Exception as exc:
            self.logger.debug("Failed to parse roster row: %s", exc)
            return None

    def _fetch_player_profile(self, player: dict[str, Any]) -> None:
        """
        Visit a player's profile page and merge all available fields
        directly into the player dict (in place).

        Fields added (when present on the page):
            full_name, player_image, team_logo, club_slug,
            jersey_number_profile, profile_<label> for every
            detail item in the player status section.

        Args:
            player: The player dict to enrich. Modified in place.
        """
        url = player.get("player_url")
        if not url:
            return

        try:
            html = self._browser_get(url)
            soup = BeautifulSoup(html, "html.parser")

            masthead = soup.select_one(".mls-o-masthead")
            if masthead:
                self._parse_masthead(masthead, player)

            details = soup.select_one(".mls-l-module--player-status-details")
            if details:
                self._parse_player_details(details, player)

        except Exception as exc:
            self.logger.warning(
                "Failed to fetch profile for %s (%s): %s",
                player.get("player_name"), url, exc,
            )

    def _parse_masthead(self, masthead, player: dict[str, Any]) -> None:
        """
        Extract image, club slug, and jersey number from the profile masthead.

        Args:
            masthead: The .mls-o-masthead BeautifulSoup element.
            player:   Player dict to enrich in place.
        """
        try:
            img = masthead.select_one(".mls-o-masthead__branded-image img")
            if img:
                player["player_image"] = img.get("src")
                if not player.get("full_name"):
                    player["full_name"] = img.get("alt", "").strip()

            club_logo_img = masthead.select_one(".mls-o-masthead__club-logo img")
            if club_logo_img:
                player["team_logo"] = club_logo_img.get("src")

            club_link = masthead.select_one("a.mls-o-masthead__club-logo")
            if club_link:
                match = re.search(r"/clubs/([^/]+)/", club_link.get("href", ""))
                if match:
                    player["club_slug"] = match.group(1)

            info = masthead.select_one(".mls-o-masthead__info-wrapper")
            if info:
                jersey_match = re.search(r"#(\d+)", info.get_text(" ", strip=True))
                if jersey_match:
                    player["jersey_number_profile"] = jersey_match.group(1)

        except Exception as exc:
            self.logger.debug("Failed to parse masthead: %s", exc)

    def _parse_player_details(self, section, player: dict[str, Any]) -> None:
        """
        Extract every label/value pair from the player status details section.

        Each item renders as:
            <div class="mls-l-module--player-status-details__info">
                <h3>Date of Birth</h3>
                <span>June 24, 1987</span>
            </div>

        All values are stored as profile_<normalised_label>, e.g.:
            profile_date_of_birth, profile_nationality, profile_height, etc.

        Args:
            section: The .mls-l-module--player-status-details element.
            player:  Player dict to enrich in place.
        """
        try:
            for item in section.select(".mls-l-module--player-status-details__info"):
                label_elem = item.select_one("h3")
                value_elem = item.select_one("span")
                if not (label_elem and value_elem):
                    continue

                label = label_elem.get_text(strip=True)
                value = value_elem.get_text(" ", strip=True)
                key = self._normalise_key(label)

                if key and value:
                    player[f"profile_{key}"] = value

        except Exception as exc:
            self.logger.debug("Failed to parse player details: %s", exc)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_key(text: str) -> str:
        """
        Convert a label string into a safe dict key.

        Examples:
            "Date of Birth" → "date_of_birth"
            "Nationality/Int'l"  → "nationality_intl"
        """
        if not text:
            return ""
        key = re.sub(r"[/]", " ", text)          # treat slash as word boundary first
        key = re.sub(r"[^\w\s]", "", key.lower())
        key = re.sub(r"\s+", "_", key.strip())
        return key