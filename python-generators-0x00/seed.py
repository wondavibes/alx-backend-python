import csv
import os
import uuid
import sys
from decimal import Decimal
from pathlib import Path

try:
    import mysql.connector
    from mysql.connector import errorcode
except Exception as exc:
    print(
        "Missing dependency: mysql-connector-python. Install with: pip install mysql-connector-python"
    )
    raise


def connect_db(host=None, user=None, password=None, port=None):
    """Connect to MySQL server (no database specified).

    Returns a mysql.connector connection.
    """
    host = host or os.environ.get("DB_HOST", "localhost")
    user = user or os.environ.get("DB_USER", "root")
    password = password if password is not None else os.environ.get("DB_PASSWORD", "")
    port = port or int(os.environ.get("DB_PORT", 3306))

    conn = mysql.connector.connect(host=host, user=user, password=password, port=port)
    return conn


def create_database(connection):
    """Create the ALX_prodev database if it does not exist."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            "CREATE DATABASE IF NOT EXISTS ALX_prodev DEFAULT CHARACTER SET 'utf8mb4'"
        )
        connection.commit()
    finally:
        cursor.close()


def connect_to_prodev(host=None, user=None, password="Dhammy14", port=None):
    """Connect to the ALX_prodev database. Creates the database if needed."""
    # Connect to server first
    conn = connect_db(host=host, user=user, password="Dhammy14", port=port)
    try:
        create_database(conn)
    finally:
        conn.close()

    # Now connect to the database
    host = host or os.environ.get("DB_HOST", "localhost")
    user = user or os.environ.get("DB_USER", "root")
    password = password if password is not None else os.environ.get("DB_PASSWORD", "")
    port = port or int(os.environ.get("DB_PORT", 3306))

    conn_db = mysql.connector.connect(
        host=host, user=user, password=password, port=port, database="ALX_prodev"
    )
    return conn_db


def create_table(connection):
    """Create the user_data table if it does not exist.

    Fields:
    - user_id CHAR(36) PRIMARY KEY
    - name VARCHAR(255) NOT NULL
    - email VARCHAR(255) NOT NULL
    - age DECIMAL(5,2) NOT NULL
    """
    create_table_sql = (
        "CREATE TABLE IF NOT EXISTS user_data ("
        "user_id CHAR(36) NOT NULL PRIMARY KEY, "
        "name VARCHAR(255) NOT NULL, "
        "email VARCHAR(255) NOT NULL, "
        "age DECIMAL(5,2) NOT NULL, "
        "INDEX (user_id)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        connection.commit()
    finally:
        cursor.close()


def insert_data(connection, data):
    """Insert a single row of data if it does not already exist (by email).

    `data` should be a dict with keys: name, email, age
    """
    cursor = connection.cursor()
    try:
        # Check existence by email
        cursor.execute(
            "SELECT user_id FROM user_data WHERE email = %s", (data["email"],)
        )
        row = cursor.fetchone()
        if row:
            print(f"Skipping existing email: {data['email']}")
            return False

        user_id = str(uuid.uuid4())
        # Ensure age is Decimal
        age_val = Decimal(data["age"]) if data["age"] != "" else Decimal(0)

        insert_sql = (
            "INSERT INTO user_data (user_id, name, email, age) VALUES (%s, %s, %s, %s)"
        )
        cursor.execute(insert_sql, (user_id, data["name"], data["email"], age_val))
        connection.commit()
        print(f"Inserted: {data['email']} (user_id={user_id})")
        return True
    finally:
        cursor.close()


def load_csv_rows(csv_path):
    """Yield rows from CSV as dictionaries with keys name,email,age"""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Normalize keys to expected ones
            yield {
                "name": r.get("name") or r.get("Name"),
                "email": r.get("email") or r.get("Email"),
                "age": r.get("age") or r.get("Age"),
            }


def main():
    # Determine CSV path relative to repo root
    repo_root = Path(__file__).resolve().parent
    csv_rel = repo_root / "python-generators-0x00" / "user_data.csv"
    if not csv_rel.exists():
        print(f"CSV file not found at: {csv_rel}")
        sys.exit(1)

    # Connect and prepare DB
    print("Connecting to MySQL server...")
    try:
        conn = connect_to_prodev()
    except mysql.connector.Error as err:
        print(f"Error connecting/creating database: {err}")
        sys.exit(1)

    try:
        create_table(conn)

        inserted = 0
        for row in load_csv_rows(csv_rel):
            # Basic validation
            if not row["name"] or not row["email"] or row["age"] is None:
                print(f"Skipping invalid row: {row}")
                continue
            try:
                if insert_data(conn, row):
                    inserted += 1
            except mysql.connector.Error as err:
                print(f"Failed to insert {row}: {err}")

        print(f"Done. Inserted {inserted} new rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
