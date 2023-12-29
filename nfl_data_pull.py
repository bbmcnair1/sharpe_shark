import pandas as pd
import nfl_data_py as nfl

save_path = '../data_dump/nfl_pbp_data/'

years = list(range(2008, 2024))
raw_pbp_data = nfl.import_pbp_data(years=years, columns=None, downcast=False)
raw_pbp_data['game_date'] = pd.to_datetime(raw_pbp_data['game_date'])
raw_pbp_data['season'] = raw_pbp_data['game_id'].str[:4].astype(int)
seasons = raw_pbp_data['season'].drop_duplicates().tolist()

# split raw_pbp_data by year
raw_pbp_data_by_season = {}
for season in seasons:
    raw_pbp_data_by_season[season] = raw_pbp_data[raw_pbp_data['season'] == season]
    # save to csv
    raw_pbp_data_by_season[season].to_csv(save_path + f'{season}.csv', index=False)

raw_player_ids = nfl.import_ids()
raw_player_ids.to_csv(save_path + f'raw_player_ids.csv', index=False)
