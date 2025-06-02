from src.db import get_connection

# Hàm thực hiện phân mảnh ngang bảng ratings theo giá trị rating (range partitioning)
def Range_Partition(ratingstablename, numberofpartitions):
    print("Phân mảnh ngang bảng", ratingstablename, "thành", numberofpartitions, "phân mảnh")
    if numberofpartitions <= 0:
        return
    
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        # Xóa các bảng phân mảnh cũ nếu có
        cursor.execute("SHOW TABLES LIKE 'range_part%';")
        old_tables = cursor.fetchall()
        for table_tuple in old_tables:
            table_name = table_tuple[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        # Tạo bảng metadata để lưu thông tin phân mảnh
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                partition_type VARCHAR(50),
                num_partitions INT,
                range_boundaries TEXT
            );
        """)
        
        # Xóa metadata cũ về phân mảnh range
        cursor.execute("DELETE FROM metadata WHERE partition_type = 'range';")
        
        # Tính toán các khoảng phân mảnh dựa trên số lượng phân mảnh
        max_rating = 5.0
        min_rating = 0.0
        delta = (max_rating - min_rating) / numberofpartitions
        boundaries = [min_rating + i * delta for i in range(numberofpartitions + 1)]
        
        # Lưu thông tin boundaries vào metadata
        boundaries_str = ",".join(str(round(b, 2)) for b in boundaries)
        
        cursor.execute("""
            INSERT INTO metadata (partition_type, num_partitions, range_boundaries)
            VALUES (%s, %s, %s);
        """, ('range', numberofpartitions, boundaries_str))
        
        # Tạo các bảng phân mảnh và chèn dữ liệu vào từng phân mảnh
        for i in range(numberofpartitions):
            table_name = f"range_part{i}"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    UserID INT,
                    MovieID INT,
                    Rating FLOAT,
                    UNIQUE(UserID, MovieID, Rating)
                );
            """)
            cursor.execute(f"DELETE FROM {table_name};")
            
            # Phân biệt điều kiện cho phân mảnh đầu tiên và các phân mảnh còn lại
            if i == 0:
                cursor.execute(f"""
                    INSERT IGNORE INTO {table_name} (UserID, MovieID, Rating)
                    SELECT UserID, MovieID, Rating FROM {ratingstablename}
                    WHERE Rating >= %s AND Rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
            else:
                cursor.execute(f"""
                    INSERT IGNORE INTO {table_name} (UserID, MovieID, Rating)
                    SELECT UserID, MovieID, Rating FROM {ratingstablename}
                    WHERE Rating > %s AND Rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
        
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

# Hàm chèn một bản ghi mới vào bảng gốc và đúng phân vùng range tương ứng
def Range_Insert(ratingstablename, userid, itemid, rating):
    # Kiểm tra dữ liệu đầu vào hợp lệ
    if not (0 <= rating <= 5):
        raise Exception("Rating must be between 0 and 5")
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        # Chèn vào bảng gốc, xử lý ngoại lệ nếu trùng bộ 3 giá trị
        try:
            cursor.execute(f"""
                INSERT INTO {ratingstablename} (UserID, MovieID, Rating)
                VALUES (%s, %s, %s);
            """, (userid, itemid, rating))
        except Exception as insert_main_ex:
            # Kiểm tra lỗi trùng lặp (Duplicate entry)
            if 'Duplicate' in str(insert_main_ex) or 'duplicate' in str(insert_main_ex):
                print(f"Bản ghi (UserID={userid}, MovieID={itemid}, Rating={rating}) đã tồn tại trong bảng {ratingstablename}.")
                return
            else:
                raise insert_main_ex
        
        # Lấy thông tin phân mảnh từ metadata
        cursor.execute("SELECT num_partitions, range_boundaries FROM metadata WHERE partition_type = 'range';")
        result = cursor.fetchone()
        if not result:
            raise Exception("No range partition metadata found.")
        
        num_partitions, boundaries_str = result[0], result[1]
        boundaries = [float(x) for x in boundaries_str.split(",")]
        
        # Xác định phân vùng phù hợp và chèn vào bảng phân mảnh tương ứng
        for i in range(num_partitions):
            if boundaries[i] <= rating <= boundaries[i + 1]:
                table_name = f"range_part{i}"
                print("Chèn dữ liệu vào phân vùng", table_name, "với rating", rating)
                try:
                    cursor.execute(f"""
                        INSERT INTO {table_name} (UserID, MovieID, Rating)
                        VALUES (%s, %s, %s);
                    """, (userid, itemid, rating))
                except Exception as insert_part_ex:
                    if 'Duplicate' in str(insert_part_ex) or 'duplicate' in str(insert_part_ex):
                        print(f"Bản ghi (UserID={userid}, MovieID={itemid}, Rating={rating}) đã tồn tại trong bảng {table_name}.")
                        return
                    else:
                        raise insert_part_ex
                break
        
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
