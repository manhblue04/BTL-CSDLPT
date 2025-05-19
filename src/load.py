from src.db import get_connection

def LoadRatings(file_path):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Ratings (
            UserID INT,
            MovieID INT,
            Rating FLOAT
        );
    """)

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split("::")
            if len(parts) >= 3:
                user_id = int(parts[0])
                movie_id = int(parts[1])
                rating = float(parts[2])
                cursor.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
                               (user_id, movie_id, rating))

    conn.commit()
    cursor.close()
