"""
Connector class to the Fantasy Premier League API
"""
import requests

class FantasyPLAPIClient:

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.gameweeks = None
        self.teams = None
        self.players = None

    def get_base_info(self, bootstrap_path: str):
        """
        Gets the base FPL info

        Attributes:
            gameweeks: gameweeks data dictionary
            teams: teams data dictionary
            players: players data dictionary

        Args:
            bootstrap_path: Static URL to retrieve base info 
        """

        bootstrap_url = f"{self.base_url}{bootstrap_path}"

        response = requests.get(url=bootstrap_url, timeout=60)

        if response is None:
            raise Exception(f"Failed to get a response from Fantasy PL API at {bootstrap_path}.")

        if response.status_code != 200:
            raise Exception(f'Failed to extract data from Fantasy PL API at {bootstrap_path}. \
                            Status Code: {response.status_code}. Response: {response.text}')

        self.gameweeks = response.json().get("events")
        self.teams = response.json().get("teams")
        self.players = response.json().get("elements")

        return self

    def get_fixtures(self, fixtures_path: str) -> list:
        """
        Gets the fixtures data

        Args:
            fixtures_path: relative path of the fixtures API resource

        Returns:
            List of fixtures
        """

        fixtures_url = f"{self.base_url}{fixtures_path}"

        response = requests.get(url=fixtures_url, timeout=60)

        if response is None:
            raise Exception(f"Failed to get a response from Fantasy PL API at {fixtures_path}.")

        if response.status_code != 200:
            raise Exception(f'Failed to extract data from Fantasy PL API at {fixtures_path}. \
                            Status Code: {response.status_code}. Response: {response.text}')

        return response.json()

    def get_player_stats(self, player_path: str) -> list:
        """
        Gets the data for a single player

        Args:
            player_path: relative path of the player stats API resource for a single player

        Returns:
            List of stats for the player
        """

        player_url = f"{self.base_url}{player_path}"

        response = requests.get(url=player_url, timeout=60)

        if response is None:
            raise Exception(f"Failed to get a response from Fantasy PL API at {player_url}.")

        if response.status_code != 200:
            raise Exception(f'Failed to extract data from Fantasy PL API at {player_url}. \
                            Status Code: {response.status_code}. Response: {response.text}')

        return response.json().get("history")
