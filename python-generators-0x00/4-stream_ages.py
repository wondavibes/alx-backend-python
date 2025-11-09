from .seed import connect_to_prodev


def stream_user_ages():
    """Generator that yields user ages one by one from the user_data table."""
    conn = connect_to_prodev()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT age FROM user_data")
        for (age,) in cursor:  # Loop 1
            yield float(age)
    finally:
        cursor.close()
        conn.close()


def calculate_average_age():
    """Calculates and prints the average age using the stream_user_ages generator."""
    total = 0
    count = 0
    for age in stream_user_ages():  # Loop 2
        total += age
        count += 1

    average = total / count if count > 0 else 0
    print(f"Average age of users: {average:.2f}")
