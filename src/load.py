from src.db import get_connection

# Hàm để nạp dữ liệu ratings từ file vào bảng Ratings trong database
def LoadRatings(file_path):
    # Lấy kết nối đến database
    conn = get_connection()
    cursor = conn.cursor()

    # Tạo bảng Ratings nếu chưa tồn tại
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Ratings (
            UserID INT,
            MovieID INT,
            Rating FLOAT
        );
    """)

    # Đọc file dữ liệu ratings
    with open(file_path, 'r') as file:
        for line in file:
            # Tách các trường từ dòng dữ liệu, định dạng: UserID::MovieID::Rating
            parts = line.strip().split("::")
            if len(parts) >= 3:
                user_id = int(parts[0])
                movie_id = int(parts[1])
                rating = float(parts[2])
                # Chèn dữ liệu vào bảng Ratings
                cursor.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s);",
                               (user_id, movie_id, rating))

    # Lưu thay đổi và đóng kết nối
    conn.commit()
    cursor.close()
