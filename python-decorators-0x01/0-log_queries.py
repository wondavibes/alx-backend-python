import sqlite3
import functools
from datetime import datetime  # ✅ Required for timestamping


def log_queries():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            query = kwargs.get("query") or (args[0] if args else None)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if query:
                print(f"[{timestamp}] Executing SQL Query: {query}")
            else:
                print(f"[{timestamp}] No SQL query found in arguments.")
            return func(*args, **kwargs)

        return wrapper

    return decorator


@log_queries()
def fetch_all_users(query):
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results


# Fetch users while logging the query with timestamp
users = fetch_all_users(query="SELECT * FROM users")
