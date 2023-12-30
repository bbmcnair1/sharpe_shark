from nfl_data_pull import *


def load_data(file_path: str) -> pd.DataFrame:
    return pd.read_pickle(file_path)


def get_latest_game_id(df: pd.DataFrame) -> str:
    df_sorted = df.sort_values('game_date', ascending=False)
    return df_sorted['game_id'].iloc[0]


def filter_data_by_game_id(df: pd.DataFrame, game_id: str) -> pd.DataFrame:
    return df[df['game_id'] == game_id]


def basic_eda(df: pd.DataFrame):
    print(df.info())
    print(df.describe())

    print('Number of Games in Sample:', df['game_id'].nunique())
    print('Game Date Range:', df['game_date'].min(), 'to', df['game_date'].max())
    print('Number of Teams Played in Sample:', df['posteam'].nunique())
    print('Teams Played in Sample:', [x for x in df['posteam'].unique() if x is not None])
    players = (df['passer_player_name'].unique().tolist()
               + df['rusher_player_name'].unique().tolist()
               + df['receiver_player_name'].unique().tolist())
    players = list(set(players))
    print('Number of Skill Players Played:', len(players))
    print('Top Passers by Attempts:', df['passer_player_name'].value_counts().head(2))
    print('Top Rushers by Attempts:', df['rusher_player_name'].value_counts().head(3))
    print('Top Receivers by Targets:', df['receiver_player_name'].value_counts().head(4))
    print('Number of Plays:', len(df))
    print('Number of Rushes:', len(df[df['play_type_nfl'] == 'RUSH']))
    print('Number of Passes:', len(df[df['play_type_nfl'] == 'PASS']))


def main(file_path: str = '../data_dump/nfl_pbp_data/2023.pkl') -> None:
    df = load_data(file_path)
    latest_game_id = get_latest_game_id(df)
    df_latest_game = filter_data_by_game_id(df, latest_game_id)
    basic_eda(df_latest_game)


if __name__ == '__main__':
    main()
