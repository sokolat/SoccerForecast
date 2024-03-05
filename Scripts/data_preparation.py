import sqlite3
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import argparse

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

def clean_data(match_df, team_attrb_df, do_normalize=False):
    if all(df is not None for df in [match_df, team_attrb_df]):
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
        #if do_normalize:
        return merged_df
    else:
        return None

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--do_normalize', action='store_true', help='Normalize the data')
    parser.add_argument('--file_path', type=str, default='database.sqlite', help='Path to the SQLite database file')
    args = parser.parse_args()
    conn = connect_to_database(args.file_path)
    if conn is not None:
        match_df, team_attrb_df = read_data(conn)
        merged_df = clean_data(match_df, team_attrb_df, do_normalize=args.do_normalize)
        #if merged_df is not None:
            #merged_df.to_csv('cleaned_data.csv', index=False)
        conn.close()

if __name__ == "__main__":
    main()
