"""Unittests for class soccerdata.SoFIFA."""

import pandas as pd

from soccerdata.sofifa import SoFIFA


async def test_read_players(sofifa_bundesliga: SoFIFA) -> None:
    """It should use the replacement names from teamname_replacements.json."""
    await sofifa_bundesliga._initialize_versions()
    assert isinstance(await sofifa_bundesliga.read_players(team="FC Bayern München"), pd.DataFrame)


async def test_read_players_replacement(sofifa_bundesliga: SoFIFA) -> None:
    """It should use the replacement names from teamname_replacements.json."""
    await sofifa_bundesliga._initialize_versions()
    assert isinstance(await sofifa_bundesliga.read_players(team="FC Bayern Munich"), pd.DataFrame)


async def test_read_team_ratings(sofifa_bundesliga: SoFIFA) -> None:
    """It should return a dataframe with the team ratings."""
    await sofifa_bundesliga._initialize_versions()
    assert isinstance(await sofifa_bundesliga.read_team_ratings(), pd.DataFrame)


async def test_read_player_ratings(sofifa_bundesliga: SoFIFA) -> None:
    """It should return a dataframe with the player ratings."""
    await sofifa_bundesliga._initialize_versions()
    assert isinstance(await sofifa_bundesliga.read_player_ratings(player=189596), pd.DataFrame)
