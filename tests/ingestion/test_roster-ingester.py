"""
tests/ingestion/test_roster_ingester.py

Testing strategy for RosterIngester
-----------------------------------
Playwright cannot be easily spun up in unit tests, so we mock _browser_get()
everywhere. This is the right approach — _browser_get() is the seam between
the scraping logic and the browser, exactly like patching requests.Session.get
in the salary tests.

All tests validate the parsing logic (HTML → dict → DataFrame) in isolation
from the browser.
"""

import pytest
import pandas as pd

from unittest.mock import MagicMock, patch, call
from ingestion.roster_ingester import RosterIngester


# ------------------------------------------------------------------
# HTML fixtures — minimal but structurally realistic
# ------------------------------------------------------------------

PLAYERS_PAGE_HTML = """
<html><body>
  <a href="/clubs/inter-miami-cf/roster/">Inter Miami</a>
  <a href="/clubs/lafc/roster/">LAFC</a>
  <a href="/clubs/inter-miami-cf/roster/">Inter Miami duplicate</a>
</body></html>
"""

ROSTER_PAGE_HTML = """
<html><body>
  <table>
    <thead>
      <tr>
        <th>Player</th>
        <th>Jersey #</th>
        <th>Position</th>
        <th>Roster Category</th>
        <th>Player Status</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>
          <a class="mls-o-table__href" href="/players/lionel-messi/">
            <img src="https://img.mlssoccer.com/messi_thumb.jpg" />
            <span class="mls-o-table__player-name">Lionel Messi</span>
          </a>
        </td>
        <td>10</td>
        <td>F</td>
        <td>Senior</td>
        <td>Active</td>
      </tr>
      <tr>
        <td>
          <a class="mls-o-table__href" href="/players/jordi-alba/">
            <span class="mls-o-table__player-name">Jordi Alba</span>
          </a>
        </td>
        <td>18</td>
        <td>D</td>
        <td>Senior</td>
        <td>Active</td>
      </tr>
    </tbody>
  </table>
</html>
"""

ROSTER_PAGE_NO_PLAYER_HEADER = """
<html><body>
  <table>
    <thead><tr><th>Foo</th><th>Bar</th></tr></thead>
    <tbody><tr><td>x</td><td>y</td></tr></tbody>
  </table>
</body></html>
"""

PROFILE_PAGE_HTML = """
<html><body>
  <div class="mls-o-masthead">
    <div class="mls-o-masthead__branded-image">
      <img src="https://img.mlssoccer.com/messi_full.jpg" alt="Lionel Messi" />
    </div>
    <a class="mls-o-masthead__club-logo" href="/clubs/inter-miami-cf/overview/">
      <img src="https://img.mlssoccer.com/miami_logo.png" />
    </a>
    <div class="mls-o-masthead__info-wrapper">#10 Forward</div>
  </div>
  <div class="mls-l-module--player-status-details">
    <div class="mls-l-module--player-status-details__info">
      <h3>Date of Birth</h3>
      <span>June 24, 1987</span>
    </div>
    <div class="mls-l-module--player-status-details__info">
      <h3>Nationality</h3>
      <span>Argentina</span>
    </div>
    <div class="mls-l-module--player-status-details__info">
      <h3>Height</h3>
      <span>5'7"</span>
    </div>
  </div>
</body></html>
"""


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def ingester(tmp_path):
    return RosterIngester(
        navigation_delay=0,     # no sleeping in tests
        raw_data_dir=str(tmp_path),
    )


# ------------------------------------------------------------------
# Initialisation
# ------------------------------------------------------------------

class TestInit:
    def test_source_name(self, ingester):
        assert ingester.source_name == "rosters"

    def test_default_delay_is_zero(self, ingester):
        # Baseingester.get() should not sleep — Playwright handles pacing
        assert ingester.default_delay == 0.0

    def test_browser_objects_start_as_none(self, ingester):
        assert ingester._playwright is None
        assert ingester._browser is None
        assert ingester._page is None


# ------------------------------------------------------------------
# _discover_teams
# ------------------------------------------------------------------

class TestDiscoverTeams:
    def test_returns_correct_team_count(self, ingester):
        with patch.object(ingester, "_browser_get", return_value=PLAYERS_PAGE_HTML):
            teams = ingester._discover_teams()
        assert len(teams) == 2  # duplicate is deduplicated

    def test_team_has_expected_keys(self, ingester):
        with patch.object(ingester, "_browser_get", return_value=PLAYERS_PAGE_HTML):
            teams = ingester._discover_teams()
        assert {"name", "slug", "roster_url"} == set(teams[0].keys())

    def test_roster_url_is_absolute(self, ingester):
        with patch.object(ingester, "_browser_get", return_value=PLAYERS_PAGE_HTML):
            teams = ingester._discover_teams()
        assert all(t["roster_url"].startswith("https://") for t in teams)

    def test_slug_extracted_correctly(self, ingester):
        with patch.object(ingester, "_browser_get", return_value=PLAYERS_PAGE_HTML):
            teams = ingester._discover_teams()
        slugs = {t["slug"] for t in teams}
        assert "inter-miami-cf" in slugs
        assert "lafc" in slugs

    def test_deduplicates_repeated_links(self, ingester):
        # PLAYERS_PAGE_HTML contains inter-miami-cf twice
        with patch.object(ingester, "_browser_get", return_value=PLAYERS_PAGE_HTML):
            teams = ingester._discover_teams()
        slugs = [t["slug"] for t in teams]
        assert slugs.count("inter-miami-cf") == 1


# ------------------------------------------------------------------
# _parse_roster_row
# ------------------------------------------------------------------

class TestParseRosterRow:
    def _get_rows_and_headers(self):
        """Parse ROSTER_PAGE_HTML and return first data row + headers."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(ROSTER_PAGE_HTML, "html.parser")
        table = soup.find("table")
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        rows = table.find_all("tr")[1:]
        team = {"name": "Inter Miami Cf", "slug": "inter-miami-cf"}
        return rows, headers, team

    def test_extracts_player_name(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert result["player_name"] == "Lionel Messi"

    def test_extracts_player_url(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert "players/lionel-messi" in result["player_url"]

    def test_player_url_is_absolute(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert result["player_url"].startswith("https://")

    def test_extracts_position(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert result["position"] == "F"

    def test_extracts_jersey_number(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert result["jersey_number"] == "10"

    def test_team_name_and_slug_attached(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert result["team_name"] == "Inter Miami Cf"
        assert result["team_slug"] == "inter-miami-cf"

    def test_thumb_extracted_when_present(self, ingester):
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[0], rows[0].find_all("td"), headers, team)
        assert result["player_image_thumb"] is not None

    def test_thumb_is_none_when_absent(self, ingester):
        # Second row (Jordi Alba) has no <img>
        rows, headers, team = self._get_rows_and_headers()
        result = ingester._parse_roster_row(rows[1], rows[1].find_all("td"), headers, team)
        assert result["player_image_thumb"] is None

    def test_returns_none_when_no_player_link(self, ingester):
        from bs4 import BeautifulSoup
        html = "<tr><td>No link here</td></tr>"
        row = BeautifulSoup(html, "html.parser").find("tr")
        result = ingester._parse_roster_row(row, row.find_all("td"), ["player"], {"name": "X", "slug": "x"})
        assert result is None


# ------------------------------------------------------------------
# _parse_masthead
# ------------------------------------------------------------------

class TestParseMasthead:
    def _get_masthead(self):
        from bs4 import BeautifulSoup
        return BeautifulSoup(PROFILE_PAGE_HTML, "html.parser").select_one(".mls-o-masthead")

    def test_extracts_full_image(self, ingester):
        player = {}
        ingester._parse_masthead(self._get_masthead(), player)
        assert player["player_image"] == "https://img.mlssoccer.com/messi_full.jpg"

    def test_extracts_full_name_from_img_alt(self, ingester):
        player = {}
        ingester._parse_masthead(self._get_masthead(), player)
        assert player["full_name"] == "Lionel Messi"

    def test_does_not_overwrite_existing_full_name(self, ingester):
        player = {"full_name": "Already Set"}
        ingester._parse_masthead(self._get_masthead(), player)
        assert player["full_name"] == "Already Set"

    def test_extracts_club_slug(self, ingester):
        player = {}
        ingester._parse_masthead(self._get_masthead(), player)
        assert player["club_slug"] == "inter-miami-cf"

    def test_extracts_jersey_number(self, ingester):
        player = {}
        ingester._parse_masthead(self._get_masthead(), player)
        assert player["jersey_number_profile"] == "10"

    def test_extracts_team_logo(self, ingester):
        player = {}
        ingester._parse_masthead(self._get_masthead(), player)
        assert "miami_logo.png" in player["team_logo"]


# ------------------------------------------------------------------
# _parse_player_details
# ------------------------------------------------------------------

class TestParsePlayerDetails:
    def _get_details_section(self):
        from bs4 import BeautifulSoup
        return BeautifulSoup(PROFILE_PAGE_HTML, "html.parser").select_one(
            ".mls-l-module--player-status-details"
        )

    def test_extracts_date_of_birth(self, ingester):
        player = {}
        ingester._parse_player_details(self._get_details_section(), player)
        assert player["profile_date_of_birth"] == "June 24, 1987"

    def test_extracts_nationality(self, ingester):
        player = {}
        ingester._parse_player_details(self._get_details_section(), player)
        assert player["profile_nationality"] == "Argentina"

    def test_extracts_height(self, ingester):
        player = {}
        ingester._parse_player_details(self._get_details_section(), player)
        assert player["profile_height"] == "5'7\""

    def test_all_details_use_profile_prefix(self, ingester):
        player = {}
        ingester._parse_player_details(self._get_details_section(), player)
        profile_keys = [k for k in player if k.startswith("profile_")]
        assert len(profile_keys) == 3


# ------------------------------------------------------------------
# _normalise_key
# ------------------------------------------------------------------

class TestNormaliseKey:
    def test_lowercases(self, ingester):
        assert ingester._normalise_key("Date of Birth") == "date_of_birth"

    def test_replaces_spaces_with_underscores(self, ingester):
        assert ingester._normalise_key("First Name") == "first_name"

    def test_strips_punctuation(self, ingester):
        assert ingester._normalise_key("Nationality/Int'l") == "nationality_intl"

    def test_empty_string_returns_empty(self, ingester):
        assert ingester._normalise_key("") == ""


# ------------------------------------------------------------------
# _scrape_team_roster — integration of parsing steps
# ------------------------------------------------------------------

class TestScrapeTeamRoster:
    def test_returns_correct_player_count(self, ingester):
        team = {"name": "Inter Miami Cf", "slug": "inter-miami-cf", "roster_url": "https://x.com"}
        with patch.object(ingester, "_browser_get", return_value=ROSTER_PAGE_HTML):
            with patch.object(ingester, "_fetch_player_profile"):  # skip profile fetching
                players = ingester._scrape_team_roster(team)
        assert len(players) == 2

    def test_returns_empty_when_no_tables(self, ingester):
        team = {"name": "X", "slug": "x", "roster_url": "https://x.com"}
        with patch.object(ingester, "_browser_get", return_value="<html><body></body></html>"):
            players = ingester._scrape_team_roster(team)
        assert players == []

    def test_skips_table_without_player_header(self, ingester):
        team = {"name": "X", "slug": "x", "roster_url": "https://x.com"}
        with patch.object(ingester, "_browser_get", return_value=ROSTER_PAGE_NO_PLAYER_HEADER):
            players = ingester._scrape_team_roster(team)
        assert players == []

    def test_deduplicates_players_across_tables(self, ingester):
        # Two identical tables — same player should appear only once
        double_html = ROSTER_PAGE_HTML.replace("</table>", "</table>") + ROSTER_PAGE_HTML
        team = {"name": "Inter Miami Cf", "slug": "inter-miami-cf", "roster_url": "https://x.com"}
        with patch.object(ingester, "_browser_get", return_value=double_html):
            with patch.object(ingester, "_fetch_player_profile"):
                players = ingester._scrape_team_roster(team)
        names = [p["player_name"] for p in players]
        assert names.count("Lionel Messi") == 1


# ------------------------------------------------------------------
# fetch_data — top-level orchestration
# ------------------------------------------------------------------

class TestFetchData:
    def test_returns_dataframe(self, ingester):
        with patch.object(ingester, "_start_browser"):
            with patch.object(ingester, "_stop_browser"):
                with patch.object(ingester, "_discover_teams", return_value=[
                    {"name": "Inter Miami Cf", "slug": "inter-miami-cf", "roster_url": "https://x.com"}
                ]):
                    with patch.object(ingester, "_scrape_team_roster", return_value=[
                        {"player_name": "Lionel Messi", "team_slug": "inter-miami-cf"}
                    ]):
                        result = ingester.fetch_data()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_stop_browser_called_even_on_exception(self, ingester):
        with patch.object(ingester, "_start_browser"):
            with patch.object(ingester, "_stop_browser") as mock_stop:
                with patch.object(ingester, "_discover_teams", side_effect=RuntimeError("boom")):
                    with pytest.raises(RuntimeError):
                        ingester.fetch_data()

        mock_stop.assert_called_once()