import mysql.connector

def get_connection():
    conn = mysql.connector.connect(
        host='localhost',
        user='thang',       # <-- bạn cần sửa đúng user MySQL
        password='123456',   # <-- và password
        database='csdlpt'    # <-- và tên database đã tạo
    )
    return conn
