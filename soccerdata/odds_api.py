import json
import os

import requests
import pandas as pd
from ._config import (
    TEAMNAME_REPLACEMENTS,
    DATA_DIR,
    LEAGUE_DICT,
    logger,
    NOSTORE,
    NOCACHE,
)
from ._common import BaseRequestsReader
from pathlib import Path


ODDSAPI_DATADIR = DATA_DIR / "OddsApi"
ODDSAPI_API = "https://api.the-odds-api.com/v3"


class OddsApi(BaseRequestsReader):

    def __init__(
        self,
        force_cache=False,
        no_cache: bool = NOCACHE,
        no_store: bool = NOSTORE,
        data_dir: Path = ODDSAPI_DATADIR,
    ):
        self.api_key = os.environ.get("ODDSAPI_KEY", "")
        self.force_cache = force_cache
        self.no_store = no_store

        super().__init__(
            no_cache=no_cache,
            no_store=no_store,
            data_dir=data_dir,
        )

        if not self.no_store:
            self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_sports(self):
        # First get a list of in-season sports
        sports_response = requests.get(
            ODDSAPI_API + "/sports", params={"api_key": self.api_key}
        )
        sports_json = json.loads(sports_response.text)
        return sports_json

    def get_bookie_odds(self, league):
        file_path = ODDSAPI_DATADIR.joinpath(league + ".json")
        if self.force_cache and file_path.is_file():
            return pd.read_json(str(file_path), orient="records", lines=True)
        sport_key = LEAGUE_DICT[league]["OddsApi"]
        odds_response = requests.get(
            ODDSAPI_API + "/odds",
            params={
                "api_key": self.api_key,
                "sport": sport_key,
                "region": "eu",  # uk | us | eu | au
                "mkt": "h2h",  # h2h | spreads | totals
                #'bookmakers': 'pinnacle'
            },
        )

        odds_json = json.loads(odds_response.text)
        # odds_json['data'] contains a list of live and
        #   upcoming events and odds for different bookmakers.
        # Events are ordered by start time (live events are first)
        # print(
        #    'Successfully got {} events'.format(len(odds_json['data'])),
        #    'Here\'s the first event:'
        # )
        # print(odds_json['data'][0])

        table = []
        for game in odds_json["data"]:
            found_odds = False
            for site in game["sites"]:
                if site["site_key"] == "pinnacle":
                    wrong_order_odds = site["odds"]["h2h"]  # home, away, draw
                    found_odds = True
                    break
            if not found_odds:
                wrong_order_odds = game["sites"][0]["odds"][
                    "h2h"
                ]  # take odds from the first site

            home_team = game["home_team"]
            if game["teams"][0] == home_team:
                away_team = game["teams"][1]
                odds = [wrong_order_odds[0], wrong_order_odds[2], wrong_order_odds[1]]
            else:
                away_team = game["teams"][0]
                odds = [wrong_order_odds[1], wrong_order_odds[2], wrong_order_odds[0]]
            table_row = [game["id"], home_team, away_team] + odds
            table.append(table_row)

            df = pd.DataFrame(
                table,
                columns=[
                    "id",
                    "home_team",
                    "away_team",
                    "bookie_home_odds",
                    "bookie_draw_odds",
                    "bookie_away_odds",
                ],
            )
            # Check your usage
            logger.info(
                f"Remaining requests{odds_response.headers['x-requests-remaining']}"
            )
            logger.info(f"Used requests {odds_response.headers['x-requests-used']}")
            df = df.replace(
                {
                    "home_team": TEAMNAME_REPLACEMENTS,
                    "away_team": TEAMNAME_REPLACEMENTS,
                }
            )
            file_path = ODDSAPI_DATADIR.joinpath(league + ".json")
            # Check if the file exists before attempting to read it
            if file_path.is_file():
                # Read the existing DataFrame from the file
                existing_df = pd.read_json(str(file_path), orient="records", lines=True)

                # Append new rows to the existing DataFrame (df)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # Remove duplicate rows
                combined_df = combined_df.drop_duplicates(
                    subset=["id", "home_team", "away_team"], keep="last"
                )

                # Save the updated DataFrame back to the file
                combined_df.to_json(str(file_path), orient="records", lines=True)
                return combined_df
            else:
                # If the file doesn't exist, create a new file with the current DataFrame
                df.to_json(str(file_path), orient="records", lines=True)
                return df
