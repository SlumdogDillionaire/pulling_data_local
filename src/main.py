#!/usr/bin/env python3
"""
A script to query bitstamp data from PostgreSQL, filter by a time range,
and save results to a nested folder structure based on symbol/broker/timeframe.

Usage Examples:
1) Using CLI arguments:
    python main.py --symbol BTCUSD --broker bitstamp --timeframe 1m \
                   --start "2018-07-14T07:30:00Z" --end "2018-07-14T07:40:00Z"

2) Using the hardcoded arguments below (uncomment the relevant lines in __main__).
"""

import os
import json
import pickle
import argparse
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import psycopg2
import pandas as pd

###############################################################################
#                        CONFIG & DEFAULT PATHS
###############################################################################

# Example: store config in a 'config' folder one level above this script
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config")
DB_CONFIG_FILE = os.path.join(CONFIG_PATH, "db_config.json")

# Where data ultimately gets saved if user doesn't specify a custom path
DEFAULT_DATA_ROOT = os.path.join(os.path.dirname(__file__), "..", "data")

###############################################################################
#                         CONFIGURATION FUNCTIONS
###############################################################################

def db_config(config_file: str) -> Dict[str, Any]:
    """
    Loads PostgreSQL connection parameters from a JSON file.

    Args:
        config_file (str): Path to the JSON config file.

    Returns:
        dict: Dictionary containing DB connection parameters.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Database configuration file not found at {config_file}.")

    with open(config_file, 'r') as json_file:
        params = json.load(json_file)

    required_keys = {"host", "port", "dbname", "user", "password"}
    missing = required_keys - set(params.keys())
    if missing:
        raise ValueError(f"The config file is missing required keys: {missing}")

    return params

###############################################################################
#                               DB UTILITIES
###############################################################################

def query_postgres_db(cursor, query: str, debug: bool = False) -> List[tuple]:
    """
    Executes a PostgreSQL query using an existing cursor and returns all rows.

    Args:
        cursor: psycopg2 cursor object.
        query (str): The SQL query to execute.
        debug (bool): If True, logs the SQL command before executing.

    Returns:
        list of tuple: The query results.
    """
    if debug:
        logging.debug(f"Executing SQL command: {query}")
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error executing query: {error}")
        return []

###############################################################################
#                       FOLDER / FILE SAVING LOGIC
###############################################################################

def save_data_to_nested_folders(
    df: pd.DataFrame,
    data_root: str,
    symbol: str,
    broker: str,
    timeframe: str,
    start_dt: datetime,
    end_dt: datetime
) -> str:
    """
    Saves a DataFrame to nested folders:
      data_root/symbol/broker/timeframe/[start_end].pkl
    Returns the path to the file created.
    """

    # Create date strings, replacing colons with valid chars (Windows can't have : in filenames)
    start_str = start_dt.strftime("%Y-%m-%d_%H-%M-%S")  # e.g. 2018-07-14_07-30-00
    end_str   = end_dt.strftime("%Y-%m-%d_%H-%M-%S")    # e.g. 2018-07-14_07-40-00

    folder_path = os.path.join(data_root, symbol, broker, timeframe)
    os.makedirs(folder_path, exist_ok=True)

    file_name = f"{start_str}_{end_str}.pkl"
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, "wb") as f:
        pickle.dump(df, f)

    logging.info(f"DataFrame saved: {file_path}")
    return file_path

###############################################################################
#                            CORE FETCH FUNCTION
###############################################################################

def fetch_and_save_data(symbol: str,
                        broker: str,
                        timeframe: str,
                        start_ns: int,
                        end_ns: int,
                        data_root: str,
                        db_config_file: str,
                        debug: bool = False) -> Optional[pd.DataFrame]:
    """
    Fetches data from the 'bitstamp_data' table for a given symbol, broker, timeframe,
    and nanosecond time range. Saves the DataFrame to a nested folder path.

    Args:
        symbol (str): E.g., 'BTCUSD'.
        broker (str): E.g., 'bitstamp'.
        timeframe (str): E.g., '1m', '5m', etc.
        start_ns (int): Start time in nanoseconds (epoch).
        end_ns (int): End time in nanoseconds (epoch).
        data_root (str): Base folder to store the nested directories.
        db_config_file (str): Path to the DB config JSON.
        debug (bool): Whether to enable debug logs.

    Returns:
        pd.DataFrame or None: The resulting DataFrame if data was found, else None.
    """
    # 1) Load DB params
    try:
        params = db_config(db_config_file)
    except Exception as e:
        logging.error(f"Failed to load DB config: {e}")
        return None

    # 2) Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**params)
    except psycopg2.OperationalError as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        return None

    # 3) Build and run the query
    query = f"""
        SELECT *
        FROM bitstamp_data
        WHERE symbol = '{symbol}'
          AND timeframe = '{timeframe}'
          AND epoch_open_time >= {start_ns}
          AND epoch_open_time < {end_ns}
        ORDER BY epoch_open_time ASC
    """

    df = None
    with conn:
        with conn.cursor() as cur:
            rows = query_postgres_db(cur, query, debug=debug)
            if not rows:
                logging.warning("No data returned from the query.")
            else:
                column_names = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=column_names)
                logging.info(f"Fetched {len(df)} rows for {symbol}-{timeframe}.")

                # 4) Convert nanoseconds back to datetime for file naming
                start_dt = datetime.utcfromtimestamp(start_ns / 1_000_000_000)
                end_dt = datetime.utcfromtimestamp(end_ns / 1_000_000_000)

                # 5) Save into nested folders
                save_data_to_nested_folders(
                    df=df,
                    data_root=data_root,
                    symbol=symbol,
                    broker=broker,
                    timeframe=timeframe,
                    start_dt=start_dt,
                    end_dt=end_dt
                )

    conn.close()
    return df

###############################################################################
#                           ARGPARSE / MAIN
###############################################################################

def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments and returns them as an argparse Namespace.
    """
    parser = argparse.ArgumentParser(
        description="Query bitstamp_data table and save results to nested folders."
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Trading pair symbol (e.g., BTCUSD)."
    )
    parser.add_argument(
        "--broker",
        default="bitstamp",
        help="Broker or exchange name (default: bitstamp)."
    )
    parser.add_argument(
        "--timeframe",
        required=True,
        help="The timeframe to query (e.g., 1m, 5m, 1D)."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start datetime in ISO format (e.g. 2018-07-14T07:30:00Z)."
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End datetime in ISO format (e.g. 2018-07-14T07:40:00Z)."
    )
    parser.add_argument(
        "--config",
        default=DB_CONFIG_FILE,
        help="Path to the DB config JSON file (default: db_config.json)."
    )
    parser.add_argument(
        "--data-root",
        default=DEFAULT_DATA_ROOT,
        help="Root folder path where nested data folders/files are created."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logs."
    )
    return parser.parse_args()

def convert_to_nanoseconds(iso_string: str) -> int:
    """
    Converts an ISO datetime string to an integer of nanoseconds since epoch.
    E.g., '2018-07-14T07:30:00Z' => 1531553400000000000
    """
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)

def main(args: argparse.Namespace = None):
    """
    Main entry point.
    If 'args' is None, we parse from the command line.
    Otherwise, we assume it's an argparse.Namespace or a similar object
    with attributes: symbol, broker, timeframe, start, end, config, data_root, debug.
    """
    if args is None:
        args = parse_arguments()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    start_ns = convert_to_nanoseconds(args.start)
    end_ns = convert_to_nanoseconds(args.end)

    fetch_and_save_data(
        symbol=args.symbol,
        broker=args.broker,
        timeframe=args.timeframe,
        start_ns=start_ns,
        end_ns=end_ns,
        data_root=args.data_root,
        db_config_file=args.config,
        debug=args.debug
    )

if __name__ == "__main__":
    #### 1) Option A: Use CLI by default
    # main()

    #### 2) Option B: Run with hardcoded arguments (for quick testing)
    class HardcodedArgs:
        symbol = "BTCUSD"
        broker = "bitstamp"
        timeframe = "1m"
        start = "2018-07-14T07:30:00Z"
        end = "2018-07-14T07:40:00Z"
        config = DB_CONFIG_FILE
        data_root = os.path.join(os.path.dirname(__file__), "..", "data")  # or any other absolute path
        debug = True

    main(HardcodedArgs())
