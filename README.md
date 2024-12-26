Pulling Sample Data (Bitstamp) from PostgreSQL
This project provides a Python script that queries cryptocurrency data from a PostgreSQL database (e.g., for Bitstamp), filters that data by a time range, and saves the results as Pickle files. It’s designed to be easy to run from the command line or by providing hardcoded arguments for quick testing.

Features
Robust CLI: Command-line options for symbol, timeframe, start date, end date, output folder, and debug mode.
Hardcoded Arguments: For local testing or script-based usage, you can bypass the CLI by passing a predefined class with arguments.
Folder Structure: Dynamically organizes the output into nested directories (data_root/symbol/broker/timeframe/) with timestamped filenames.
Windows-Safe Filenames: Replaces disallowed characters (like :) in date strings to prevent path errors on Windows.
Requirements
Python 3.7+
Psycopg2 library for PostgreSQL connections
pandas for DataFrame manipulation
argparse (bundled with Python 3 by default)
pickle (bundled with Python 3 by default)
You can install the necessary packages with:

bash
Copy code
pip install -r requirements.txt
(Or just pip install psycopg2 pandas if you don’t have a requirements.txt.)

Project Structure
A typical layout might look like this:

css
Copy code
pulling_sample_data_v1/
├── config/
│   └── db_config.json
├── src/
│   └── main.py
├── data/               # (Generated at runtime to store .pkl files)
├── README.md
├── requirements.txt
└── .gitignore
db_config.json stores database credentials (host, port, dbname, user, password).
main.py is the main script that queries PostgreSQL and saves data.
data/ will be created automatically if it doesn’t exist.
Configuration
Database JSON File
Your db_config.json might look like:

json
Copy code
{
  "host": "127.0.0.1",
  "port": 5432,
  "dbname": "mydatabase",
  "user": "myuser",
  "password": "mypassword"
}
Note: For security, do not commit real credentials to a public repo. Instead, keep db_config.json out of version control or manage credentials via environment variables.

Usage
1. Command-Line (Default)
From within the src directory (or adjusting your paths accordingly), run:

bash
Copy code
python main.py \
  --symbol BTCUSD \
  --timeframe 1m \
  --start "2018-07-14T07:30:00Z" \
  --end "2018-07-14T07:40:00Z" \
  --config "path/to/db_config.json" \
  --output "path/to/data/folder" \
  --debug
--symbol: The trading symbol (e.g., BTCUSD).
--timeframe: Candle timeframe (e.g., 1m, 5m, 1D).
--start: Start datetime in ISO format, e.g. 2018-07-14T07:30:00Z.
--end: End datetime in ISO format, e.g. 2018-07-14T07:40:00Z.
--config: Path to your db_config.json file (defaults to db_config.json).
--output: Directory to store output files. Defaults to a local folder (data/).
--debug: Enables verbose logging.
2. Hardcoded Arguments (For Quick Testing)
Open main.py and scroll down to the bottom. You’ll see something like:

python
Copy code
if __name__ == "__main__":
    # main()  # CLI usage

    class HardcodedArgs:
        symbol = "BTCUSD"
        timeframe = "1m"
        start = "2018-07-14T07:30:00Z"
        end = "2018-07-14T07:40:00Z"
        config = "db_config.json"
        output = "C:\\path\\to\\data"
        debug = True

    main(HardcodedArgs())
Uncomment main(HardcodedArgs()) and comment out main() to run with these hardcoded values.
This is useful for testing or if you want to wrap your script in another automation tool.
Output Structure
When you run the script, it will create a nested folder structure (if you’re using the enhanced version with brokers, etc.) or just a single .pkl file. For example:

kotlin
Copy code
data/
 └─ BTCUSD/
     └─ bitstamp/
         └─ 1m/
             └─ 2018-07-14_07-30-00_2018-07-14_07-40-00.pkl
Note: The dates in the filename have colons replaced with - or _ to avoid Windows path issues.

Logging
Default log level is INFO.
If you specify --debug, you’ll see DEBUG-level output, including the SQL query.
Common Issues
Invalid Character in Path
Windows does not allow : in filenames. Our code replaces them automatically in the date strings.
Connection Failures
Check your db_config.json, ensure the database server is up, port is correct, etc.
No Data Returned
Might be due to an empty table, wrong symbol/timeframe, or the date range not matching any data.
Contributing
Fork the repository and create a new branch for your feature/fix.
Make your changes and ensure existing functionality remains intact.
Submit a pull request with a clear explanation of what your changes do.
License
MIT