import sqlite3
import pandas as pd
import os


def connect_to_database(file_path):
    try:
        conn = sqlite3.connect(file_path)
        print("Connection successful!")
        return conn
    except sqlite3.Error as e:
        print("Error connecting to SQLite database:", e)
        return None


def read_data(conn):
    try:
        match_df = pd.read_sql_query("SELECT * FROM Match", conn)
        team_attrb_df = pd.read_sql_query("SELECT * FROM Team_attributes", conn)
        return match_df, team_attrb_df
    except sqlite3.Error as e:
        print("Error reading data from database:", e)
        return None


def clean_data(match_df, team_attrb_df):
    if all(df is not None for df in [match_df, team_attrb_df]):
        match_df.drop_duplicates(inplace=True)
        match_df.dropna(thresh=0.9 * len(match_df), axis=1, inplace=True)
        match_df.fillna(match_df.mean(numeric_only=True).round(1), inplace=True)
        team_attrb_df.drop('buildUpPlayDribbling', axis=1, inplace=True)

        df_attrb_home_df = team_attrb_df.add_prefix('home_')
        merged_home = pd.merge(df_attrb_home_df, match_df, on='home_team_api_id')

        df_attrb_away_df = team_attrb_df.add_prefix('away_')
        merged_df = pd.merge(df_attrb_away_df, merged_home, on='away_team_api_id')

        merged_df.drop(list(merged_df.filter(regex='id')), axis=1, inplace=True)
        merged_df.rename(columns={'home_date': 'date'}, inplace=True)
        merged_df.drop('away_date', axis=1, inplace=True)

        return merged_df
    else:
        return None


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    file_path = os.path.join(parent_dir, 'database.sqlite')
    conn = connect_to_database(file_path)
    if conn is not None:
        match_df, team_attrb_df = read_data(conn)
        merged_df = clean_data(match_df, team_attrb_df)
        if merged_df is not None:
            print(merged_df.columns.tolist())
        conn.close()


if __name__ == "__main__":
    main()
