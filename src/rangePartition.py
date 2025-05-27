from src.db import get_connection

def Range_Partition(ratingstablename, numberofpartitions):
    print("Phân mảnh ngang bảng", ratingstablename, "thành", numberofpartitions, "phân mảnh")
    if numberofpartitions <= 0:
        return
    
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        cursor.execute("SHOW TABLES LIKE 'range_part%';")
        old_tables = cursor.fetchall()
        for table_tuple in old_tables:
            table_name = table_tuple[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                partition_type VARCHAR(50),
                num_partitions INT,
                range_boundaries TEXT
            );
        """)
        
        cursor.execute("DELETE FROM metadata WHERE partition_type = 'range';")
        
        max_rating = 5.0
        min_rating = 0.0
        delta = (max_rating - min_rating) / numberofpartitions
        boundaries = [min_rating + i * delta for i in range(numberofpartitions + 1)]
        
        boundaries_str = ",".join(str(round(b, 2)) for b in boundaries)
        
        cursor.execute("""
            INSERT INTO metadata (partition_type, num_partitions, range_boundaries)
            VALUES (%s, %s, %s);
        """, ('range', numberofpartitions, boundaries_str))
        
        for i in range(numberofpartitions):
            table_name = f"range_part{i}"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    UserID INT,
                    MovieID INT,
                    Rating FLOAT
                );
            """)
            cursor.execute(f"DELETE FROM {table_name};")
            
            if i == 0:
                cursor.execute(f"""
                    INSERT INTO {table_name} (UserID, MovieID, Rating)
                    SELECT UserID, MovieID, Rating FROM {ratingstablename}
                    WHERE Rating >= %s AND Rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
            else:
                cursor.execute(f"""
                    INSERT INTO {table_name} (UserID, MovieID, Rating)
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

def Range_Insert(ratingstablename, userid, itemid, rating):
    if not (0 <= rating <= 5):
        raise Exception("Rating must be between 0 and 5")
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        cursor.execute(f"""
            INSERT INTO {ratingstablename} (UserID, MovieID, Rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        cursor.execute("SELECT num_partitions, range_boundaries FROM metadata WHERE partition_type = 'range';")
        result = cursor.fetchone()
        if not result:
            raise Exception("No range partition metadata found.")
        
        num_partitions, boundaries_str = result[0], result[1]
        boundaries = [float(x) for x in boundaries_str.split(",")]
        
        for i in range(num_partitions):
            if boundaries[i] <= rating <= boundaries[i + 1]:
                table_name = f"range_part{i}"
                print("Chèn dữ liệu vào phân vùng", table_name, "với rating", rating)
                cursor.execute(f"""
                    INSERT INTO {table_name} (UserID, MovieID, Rating)
                    VALUES (%s, %s, %s);
                """, (userid, itemid, rating))
                break
        
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
    