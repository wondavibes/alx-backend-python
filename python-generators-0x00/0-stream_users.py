from .seed import connect_to_prodev

def stream_users():
    """Generator that yields rows from the user_data table one by one."""
    conn = connect_to_prodev()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        for row in cursor:
            yield row
    finally:
        cursor.close()
        conn.close()
