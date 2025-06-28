
import mysql.connector
import os
import re
from datetime import datetime

# Path to access.log
lo = os.path.join(os.path.dirname(__file__), 'web-server-access-logs', 'access.log')

# Regex for Extended Common Log Format
LOG_PATTERN = re.compile(
    r'(?P<ip>\d{1,3}(?:\.\d{1,3}){3}) - - \[(?P<datetime>[^\]]+)\] '
    r'"(?P<method>\S+)\s(?P<endpoint>\S+)[^"]*" (?P<status>\d{3}) (?P<size>\d+|-)'
)

# Max rows to insert (to stay under ~5MB)
MAX_ROWS = 12000
BATCH_SIZE = 1000  # Smaller batch size for remote connection

def parse_log_line(line):
    match = LOG_PATTERN.match(line)
    if match:
        data = match.groupdict()
        try:
            data["datetime"] = datetime.strptime(data["datetime"], '%d/%b/%Y:%H:%M:%S %z')
        except Exception:
            return None
        data["status"] = int(data["status"])
        data["size"] = int(data["size"]) if data["size"].isdigit() else 0
        return data
    return None

def get_db_connection():
    """Get database connection with error handling"""
    try:
        # Get connection details from environment variables
        # You'll need to set these based on your freesqldatabase.com account
        config = {
            'host': os.getenv("DB_HOST"),
            'user': os.getenv("DB_USER"), 
            'password': os.getenv("DB_PASS"),
            'database': os.getenv("DB_NAME"),
            'port': int(os.getenv("DB_PORT", "3306")),
            'autocommit': False,
            'connection_timeout': 60,
            'raise_on_warnings': True
        }
        
        # Remove None values
        config = {k: v for k, v in config.items() if v is not None}
        
        print(f" Connecting to {config.get('host')}:{config.get('port')}...")
        conn = mysql.connector.connect(**config)
        print(" Database connection successful!")
        return conn
        
    except mysql.connector.Error as err:
        print(f" Database connection failed: {err}")
        print("\ Make sure you've set these environment variables:")
        print("   DB_HOST=your_host_from_freesqldatabase")
        print("   DB_USER=your_username")
        print("   DB_PASS=your_password")
        print("   DB_NAME=your_database_name")
        print("   DB_PORT=3306")
        raise
    except Exception as e:
        print(f" Unexpected error: {e}")
        raise

def process_and_insert():
    batch = []
    inserted_rows = 0

    # Connect to MySQL
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Ensure table exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ip VARCHAR(45),
            datetime DATETIME,
            method VARCHAR(10),
            endpoint TEXT,
            status INT,
            size INT,
            INDEX idx_datetime (datetime),
            INDEX idx_ip (ip),
            INDEX idx_status (status)
        )
        """)
        print(" Table created/verified")

        insert_query = """
        INSERT INTO logs (ip, datetime, method, endpoint, status, size)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        # Check if log file exists
        if not os.path.exists(lo):
            print(f" Log file not found: {lo}")
            print("Please make sure the access.log file exists in the web-server-access-logs directory")
            return

        print(f" Reading log file: {lo}")
        
        with open(lo, 'r', encoding='utf-8', errors='ignore') as file:
            for i, line in enumerate(file):
                if inserted_rows >= MAX_ROWS:
                    print(f" Stopped after {inserted_rows} rows (max limit).")
                    break

                parsed = parse_log_line(line.strip())
                if parsed:
                    batch.append((
                        parsed['ip'], parsed['datetime'], parsed['method'],
                        parsed['endpoint'], parsed['status'], parsed['size']
                    ))
                    inserted_rows += 1

                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(insert_query, batch)
                        conn.commit()
                        print(f" Inserted batch of {BATCH_SIZE} (total: {inserted_rows})")
                        batch = []

                # Progress indicator for large files
                if (i + 1) % 10000 == 0:
                    print(f" Processed {i + 1} lines, inserted {inserted_rows} valid records")

            # Final batch insert
            if batch:
                cursor.executemany(insert_query, batch)
                conn.commit()
                print(f" Final insert of {len(batch)} rows.")

    except FileNotFoundError:
        print(f" Log file not found: {lo}")
    except mysql.connector.Error as err:
        print(f" Database error: {err}")
        conn.rollback()
    except Exception as e:
        print(f" Error processing file: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print(f" All done. {inserted_rows} total rows inserted.")

def test_connection():
    """Test database connection before processing"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print(" Connection test successful!")
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

def main():
    print("🚧 Starting safe log parse + insert (max 12,000 rows)...")
    print("🔍 Testing database connection first...")
    
    if test_connection():
        process_and_insert()
    else:
        print("Cannot proceed without database connection")

if __name__ == '__main__':
    main()
