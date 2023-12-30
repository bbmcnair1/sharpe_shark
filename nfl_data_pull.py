import pandas as pd
from typing import List
import os
import nfl_data_py as nfl


def check_cached_seasons(save_path: str) -> List[int]:
    current_files = os.listdir(save_path)
    cached_seasons = []
    for file in current_files:
        if file.endswith('.pkl') and file[:-4].isdigit() and len(file[:-4]) == 4:
            cached_seasons.append(int(file[:-4]))
    cached_seasons.sort()
    return cached_seasons


def determine_years_to_pull(refresh_all_years: bool, refresh_current_year: bool, cached_seasons: List[int]) -> List[int]:
    years = list(range(2008, 2024))
    if not refresh_all_years:
        years = [year for year in years if year not in cached_seasons]

    current_year, current_month = pd.to_datetime('today').year, pd.to_datetime('today').month
    current_season = current_year if current_month > 3 else current_year - 1

    if refresh_current_year and current_season not in years:
        years.append(current_season)
    return years


def pull_and_save_pbp_data(years: List[int], save_path: str) -> None:
    raw_pbp_data = nfl.import_pbp_data(years=years, columns=None, downcast=False)
    raw_pbp_data['game_date'] = pd.to_datetime(raw_pbp_data['game_date'])
    seasons = raw_pbp_data['season'].drop_duplicates().tolist()

    for season in seasons:
        season_data = raw_pbp_data[raw_pbp_data['season'] == season]
        season_data.to_pickle(save_path + f'{season}.pkl')


def pull_and_save_player_ids(save_path: str) -> None:
    raw_player_ids = nfl.import_ids()
    raw_player_ids.to_pickle(save_path + f'raw_player_ids.pkl')


def main(refresh_all_years: bool = False, refresh_current_year: bool = True, save_path: str = '../data_dump/nfl_pbp_data/') -> None:
    cached_seasons = check_cached_seasons(save_path)
    years = determine_years_to_pull(refresh_all_years, refresh_current_year, cached_seasons)
    try:
        pull_and_save_pbp_data(years, save_path)
        pull_and_save_player_ids(save_path)
        print('Data pull successful.')
    except Exception as e:
        print(f'An error occurred while attempting to refresh data: {e}')
        print('Using cached data instead.')


if __name__ == '__main__':
    main()
