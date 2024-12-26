import json
import psycopg2
import os

pairlist = ['BTCUSD']

# Path to the config folder
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config")
DB_CONFIG_FILE = os.path.join(CONFIG_PATH, "db_config.json")

def db_config():
    with open(DB_CONFIG_FILE, 'r') as json_file:
        params = json.load(json_file)
    return params


def debug_bitstamp_data():
    """
    Pulls a few lines from the 'bitstamp_data' table to verify if it really has data.
    """
    # 1. Get connection parameters
    params = db_config()  # Adjust if needed: your own function that loads JSON credentials

    # 2. Connect to PostgreSQL
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    # 3. Execute a simple query - no WHERE clause, just get a few rows
    query = "SELECT * FROM bitstamp_data LIMIT 5;"
    cur.execute(query)
    rows = cur.fetchall()

    # 4. Print out the column names
    column_names = [desc[0] for desc in cur.description]
    print("Column names in 'bitstamp_data':", column_names)

    # 5. Print out what we got
    if not rows:
        print(
            "No rows returned from the bitstamp_data table. It may be empty or you’re connected to the wrong DB.")
    else:
        print("A few rows from bitstamp_data:")
        for row in rows:
            print(row)

    # 6. Clean up
    cur.close()
    conn.close()


if __name__ == "__main__":
    debug_bitstamp_data()
