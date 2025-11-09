from .seed import connect_to_prodev


def stream_users_in_batches(batch_size):
    """Generator that yields batches of users from the user_data table."""
    conn = connect_to_prodev()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        batch = []
        for row in cursor:  #type: ignore
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
    finally:
        cursor.close()
        conn.close()


def batch_processing(batch_size):
    """Generator that yields users over the age of 25 from each batch."""
    for batch in stream_users_in_batches(batch_size):  # Loop 2
        filtered = [
            user for user in batch if float(user["age"]) > 25
        ]  # Loop 3 (list comprehension)
        yield filtered
