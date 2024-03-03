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
        return match_df
    except sqlite3.Error as e:
        print("Error reading data from database:", e)
        return None


def check_duplicates(df):
    is_dup = (~df.duplicated()).all()
    print(is_dup)


def calculate_missing_values(df):
    percent_missing = df.isnull().sum() * 100 / len(df)
    missing_value_df = pd.DataFrame({'column_name': df.columns, 'percent_missing': percent_missing})
    missing_value_df.sort_values('percent_missing', inplace=True, ascending=False)
    print(missing_value_df)


def main():
    # Get the current working directory (where your Python script is located)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Get the parent directory (one level up)
    parent_dir = os.path.dirname(current_dir)
    # Construct the full path to database.sqlite
    file_path = os.path.join(parent_dir, 'database.sqlite')
    conn = connect_to_database(file_path)
    if conn is not None:
        match_df = read_data(conn)
        if match_df is not None:
            check_duplicates(match_df)
            calculate_missing_values(match_df)
            conn.close()


if __name__ == "__main__":
    main()
