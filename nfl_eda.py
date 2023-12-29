from nfl_data_check import *

# todo: for now this pulls just the most recent game played, will want to update EDA to at least one team or full season
file_path = '../data_dump/nfl_pbp_data/2023.pkl'
df = load_data(file_path)
latest_game_id = get_latest_game_id(df)
df_latest_game = filter_data_by_game_id(df, latest_game_id)

df = df_latest_game.copy()

# begin grouping relevant columns
all_cols = df.columns.tolist()
basic_cols = all_cols[all_cols.index('play_id'): all_cols.index('yards_gained') + 1]
rush_pass_cols = all_cols[all_cols.index('rush_attempt'): all_cols.index('pass_attempt') + 1]
player_cols = all_cols[all_cols.index('passer_player_id'): all_cols.index('rushing_yards') + 1]
play_type_cols = all_cols[all_cols.index('pass_length'): all_cols.index('run_gap') + 1]
touchdown_cols = all_cols[all_cols.index('touchdown'): all_cols.index('rush_touchdown') + 1]
formation_cols = all_cols[all_cols.index('time_to_throw'): all_cols.index('defense_coverage_type') + 1]
wp_cols = all_cols[all_cols.index('wp'): all_cols.index('vegas_home_wp') + 1]
vegas_cols = all_cols[all_cols.index('result'): all_cols.index('total_line') + 1]
score_cols = all_cols[all_cols.index('total_home_score'): all_cols.index('score_differential_post') + 1]
other_cols = ['season', 'cp', 'cpoe', 'stadium', 'weather', 'play_type_nfl', 'away_score', 'home_score',
              'home_coach', 'away_coach', 'passer', 'rusher', 'receiver', 'pass', 'rush', 'first_down', 'xpass',
              'pass_oe', 'possession_team', 'offense_formation', 'offense_personnel', 'defense_personnel',
              'defenders_in_box', 'offense_players', 'defense_players']
other_cols = [col for col in other_cols if col in df.columns.tolist()]

# trim to relevant columns
cols_to_keep = basic_cols + rush_pass_cols + player_cols + play_type_cols + touchdown_cols + formation_cols + wp_cols + vegas_cols + score_cols + other_cols
assert len(cols_to_keep) == len(set(cols_to_keep))
duplicates = [col for col in cols_to_keep if cols_to_keep.count(col) > 1]
df = df.loc[:, cols_to_keep].copy()

# start some basic summaries of the game

# game details
# date, home team, away team, stadium, weather, season, coaches
sum_game = df[['season', 'week', 'season_type', 'game_date', 'home_team', 'away_team']].drop_duplicates()

# team aggregated stats
# total plays, total yards, total points, total first downs, total turnovers, total sacks, total penalties

# box score summaries
# pass summary
# rush summary
# receiving summary

# timeline summaries
# yards gained
# score differential
# win probability