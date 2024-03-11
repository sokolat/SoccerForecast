import sqlite3
import pandas as pd
import argparse
from sklearn import preprocessing


def connect_to_database(file_path):
    """
    Connect to the SQLite database.

    Parameters:
    - file_path (str): Path to the SQLite database file.

    Returns:
    - conn (sqlite3.Connection): SQLite database connection.
    """
    try:
        conn = sqlite3.connect(file_path)
        print("Connection successful!")
        return conn
    except sqlite3.Error as e:
        print("Error connecting to SQLite database:", e)
        return None


def read_data(conn):
    """
    Read data from the 'Match' and 'Team_attributes' tables in the database.

    Parameters:
    - conn (sqlite3.Connection): SQLite database connection.

    Returns:
    - match_df (pd.DataFrame): DataFrame containing 'Match' table data.
    - team_attrb_df (pd.DataFrame): DataFrame containing 'Team_attributes' table data.
    """
    try:
        match_df = pd.read_sql_query("SELECT * FROM Match", conn)
        team_attrb_df = pd.read_sql_query("SELECT * FROM Team_attributes", conn)
        return match_df, team_attrb_df
    except sqlite3.Error as e:
        print("Error reading data from database:", e)
        return None


def clean_data(match_df, team_attrb_df, do_normalize=False):
    """
    Clean and preprocess the data.

    Parameters:
    - match_df (pd.DataFrame): DataFrame containing 'Match' table data.
    - team_attrb_df (pd.DataFrame): DataFrame containing 'Team_attributes' table data.
    - do_normalize (bool): Flag to normalize the data.

    Returns:
    - merged_df (pd.DataFrame): Cleaned and merged DataFrame.
    """
    if all(df is not None for df in [match_df, team_attrb_df]):
        # Drop unnecessary columns
        match_df.drop(columns=['season', 'date'], axis=1, inplace=True)
        match_df.dropna(thresh=0.9 * len(match_df), axis=1, inplace=True)
        match_df.fillna(match_df.mean(numeric_only=True).round(1), inplace=True)

        team_attrb_df.drop(columns=['buildUpPlayDribbling', 'date'], axis=1, inplace=True)

        # Merge team attributes for home and away teams with match data
        df_attrb_home = team_attrb_df.add_prefix('home_')
        df_attrb_away = team_attrb_df.add_prefix('away_')
        merged_home = pd.merge(df_attrb_home, match_df, left_on='home_team_api_id', right_on='home_team_api_id')
        merged_df = pd.merge(df_attrb_away, merged_home, left_on='away_team_api_id', right_on='away_team_api_id')

        # Drop id columns and normalize data if needed
        merged_df.drop(list(merged_df.filter(regex='id')), axis=1, inplace=True)
        if do_normalize:
            cols_to_norm = list(merged_df.filter(regex='^(?:(?!Class).)*$'))
            merged_df[cols_to_norm] = merged_df[cols_to_norm].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

        # Create a new column for match outcome and drop goal-related columns
        merged_df['home_outcome'] = merged_df.apply(lambda x: 0 if x['away_team_goal'] > x['home_team_goal'] else (
            1 if x['home_team_goal'] > x['away_team_goal'] else 2), axis=1)
        merged_df.drop(list(merged_df.filter(regex='goal')), axis=1, inplace=True)

        # encode all categorical data
        le = preprocessing.LabelEncoder()
        colums_to_encode = list(merged_df.filter(regex='.*Class$'))
        print(colums_to_encode)
        for col in colums_to_encode:
            merged_df[col] = le.fit_transform(merged_df[col])
        return merged_df
    else:
        return None


def main():
    """
    Main function to execute the data preprocessing pipeline.
    """
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--do_normalize', action='store_true', help='Normalize the data')
    parser.add_argument('--file_path', type=str, default='database.sqlite', help='Path to the SQLite database file')
    args = parser.parse_args()

    # Connect to the database
    conn = connect_to_database(args.file_path)
    if conn is not None:
        # Read data from the database
        match_df, team_attrb_df = read_data(conn)
        # Clean and preprocess the data
        merged_df = clean_data(match_df, team_attrb_df, do_normalize=args.do_normalize)
        if merged_df is not None:
            # Split the data into train and test sets and save them as CSV files
            merged_df.head(10).to_csv('sample.csv')
            merged_df.to_csv('cleaned_data.csv', index=False)
        conn.close()


if __name__ == "__main__":
    main()
