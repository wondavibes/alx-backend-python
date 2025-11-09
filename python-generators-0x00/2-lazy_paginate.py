def paginate_users(page_size, offset):
    """Fetch a page of users starting from the given offset."""
    conn = connect_to_prodev()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT * FROM user_data LIMIT %s OFFSET %s",
            (page_size, offset),
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def lazy_paginate(page_size):
    """Generator that yields pages of users one at a time, starting from offset 0."""
    offset = 0
    while True:  # Loop 1 (only loop used)
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page
        offset += page_size
