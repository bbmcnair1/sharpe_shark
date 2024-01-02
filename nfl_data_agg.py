from nfl_data_pull import *
from nfl_data_check import *
from nfl_trim_data import *
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = 'browser'

''' ==============================================================
    A script for exploratory data analysis of NFL play-by-play data
    - trims columns and rows (might want this as specific function that can be imported elsewhere
    - summarizes game details
============================================================== '''

# todo: for now this pulls just the most recent game played, will want to update EDA to at least one team or full season
file_path = '../data_dump/nfl_pbp_data/2022.pkl'
df = load_data(file_path)
latest_game_id = get_latest_game_id(df)
df_latest_game = filter_data_by_game_id(df, latest_game_id)
player_ids = pd.read_pickle('../data_dump/nfl_pbp_data/raw_player_ids.pkl')[
    ['gsis_id', 'name', 'merge_name', 'position', 'birthdate', 'draft_year', 'height', 'weight']]

df = df_latest_game.copy()
raw_yards = df['yards_gained'].sum()
df.sort_values(['game_date', 'game_id', 'play_id'], ascending=[True, True, True], inplace=True)
df.reset_index(drop=True, inplace=True)

# trim columns and rows
df, col_dict = trim_columns(df)
df, df_row_drop = trim_rows(df, play_type_filter=['pass', 'run', 'qb_kneel'])

print('game minutes captured: ', round(df['time_elapsed'].sum() / 60, 1))
yards_captured = df['yards_gained'].sum()
print('share of raw yards captured: ', round(yards_captured / raw_yards * 100, 1))

# export df to .csv in file path
df.to_csv('../data_dump/nfl_pbp_data/sb2022.csv', index=False)
df.to_excel('../data_dump/nfl_pbp_data/sb2022.xlsx', index=False)

''' ==============================================================
    Game Details
============================================================== '''

'''
    Pre-Game Details
'''


# todo: this is game-specific, need to groupby group_cols
def gen_pre_game_summary(df: pd.DataFrame):
    columns = ['season', 'week', 'game_date', 'home_team', 'away_team', 'spread_line', 'total_line']
    pre_game = df[columns].drop_duplicates()
    pre_game['favorite'] = np.where(pre_game['spread_line'] > 0, pre_game['home_team'], pre_game['away_team'])
    pre_game['underdog'] = np.where(pre_game['spread_line'] > 0, pre_game['away_team'], pre_game['home_team'])
    pre_game = pre_game.T
    pre_game.columns = ['game_details']
    return pre_game


'''
    Team-level stats
'''


# todo: this is game-specific, need to groupby group_cols
def gen_team_level_stats(df: pd.DataFrame):
    home_team = df['home_team'].unique()[0]
    away_team = df['away_team'].unique()[0]
    home_score = df['home_score'].unique()[0]
    away_score = df['away_score'].unique()[0]
    stats = df.groupby('posteam').agg(
        total_points=('posteam_score_post', 'max'),
        total_plays=('play_id', 'count'),
        total_yards=('yards_gained', 'sum'),
        time_of_possession=('time_elapsed', 'sum'),
        first_downs=('first_down', 'sum'),
        dropbacks=('dropback', 'sum'),
        pass_attempts=('pass_attempt', 'sum'),
        pass_completions=('pass_completion', 'sum'),
        pass_yards=('passing_yards', 'sum'),
        pass_touchdowns=('pass_touchdown', 'sum'),
        scrambles=('qb_scramble', 'sum'),
        scramble_yards=('scramble_yards', 'sum'),
        designed_runs=('designed_run', 'sum'),
        designed_rushing_yards=('designed_rushing_yards', 'sum'),
        rush_touchdowns=('rush_touchdown', 'sum')
    ).reset_index()
    stats['yards_per_play'] = stats['total_yards'] / stats['total_plays']
    stats['yards_per_attempt'] = stats['pass_yards'] / stats['pass_attempts']
    stats['completion_rate'] = stats['pass_completions'] / stats['pass_attempts']
    stats['yards_per_rush'] = stats['designed_rushing_yards'] / stats['designed_runs']

    # update total_points for home and away teams mapping to posteam
    stats.loc[stats['posteam'] == home_team, 'total_points'] = home_score
    stats.loc[stats['posteam'] == away_team, 'total_points'] = away_score

    # transpose and rename columns
    stats = stats.set_index('posteam').T
    return stats


'''
    Box Score
'''


def gen_box_score(df: pd.DataFrame, group_cols: list = None):
    if group_cols is None:
        group_cols = ['season', 'week', 'game_id', 'posteam', 'defteam']
    ascending_list = [True] * len(group_cols) + [False]

    # Passing stats
    passing_stats = df[df['pass_attempt'] == 1].groupby(group_cols+['passer_player_name'], as_index=False).agg(
        pass_attempts=('pass_attempt', 'sum'),
        completions=('receiving_yards', 'count'),
        passing_yards=('passing_yards', 'sum'),
        passing_touchdowns=('pass_touchdown', 'sum')
        # Add more stats as needed
    ).sort_values(group_cols+['passing_yards'], ascending=ascending_list).reset_index(drop=True)

    # Rushing stats
    rushing_stats = df[df['rush_attempt'] == 1].groupby(group_cols+['rusher_player_name'], as_index=False).agg(
        rushing_attempts=('rush_attempt', 'sum'),
        rushing_yards=('rushing_yards', 'sum'),
        rushing_touchdowns=('rush_touchdown', 'sum')
        # Add more stats as needed
    ).sort_values(group_cols+['rushing_yards'], ascending=ascending_list).reset_index(drop=True)

    # Receiving stats
    receiving_stats = df[df['pass_attempt'] == 1].groupby(group_cols+['receiver_player_name'], as_index=False).agg(
        targets=('receiver_player_name', 'count'),
        catches=('receiving_yards', 'count'),
        receiving_yards=('receiving_yards', 'sum'),
        receiving_touchdowns=('pass_touchdown', 'sum')
        # Add more stats as needed
    ).sort_values(group_cols+['receiving_yards'], ascending=ascending_list).reset_index(drop=True)

    return passing_stats, rushing_stats, receiving_stats


'''
    Team & Player Snaps
'''


def count_player_appearances_by_game(df: pd.DataFrame, player_columns: list = None, group_cols: list = None):
    for col in group_cols:
        if col not in df.columns:
            raise ValueError(f"Column {col} not found in DataFrame")

    melted_df = df.melt(id_vars=group_cols, value_vars=player_columns, var_name='player_col', value_name='player_id')
    melted_df = melted_df.dropna(subset=['player_id'])
    grouped_df = melted_df.groupby(group_cols+['player_id']).size().reset_index(name='count')

    return grouped_df


'''
    Timeline Summaries
'''
# yards gained by team, type, player -- can do Sharpe here... ypa and std dev
# score differential
# win probability

'''
    Execute Functions
'''
group_cols = ['season', 'week', 'game_id', 'posteam', 'defteam']

pre_game = gen_pre_game_summary(df)
team_stats = gen_team_level_stats(df)

passing_stats, rushing_stats, receiving_stats = gen_box_score(df, group_cols=group_cols)
team_snaps = df.groupby(group_cols, as_index=False)['play_id'].count().rename(columns={'play_id': 'total_snaps'})
player_snaps = count_player_appearances_by_game(df, player_columns=[f'player{i}' for i in range(1, 12)], group_cols=group_cols)
player_snaps = player_snaps.merge(player_ids[['gsis_id', 'name']], how='inner', left_on='player_id', right_on='gsis_id').sort_values(['game_id', 'posteam', 'count'], ascending=[True, True, False])

'''
    Test Area
'''



