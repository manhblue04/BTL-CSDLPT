#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='duyha2k4', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_db(dbname):
    """
    Tạo một cơ sở dữ liệu bằng cách kết nối đến user và database mặc định của Postgres.
    Hàm sẽ kiểm tra xem cơ sở dữ liệu với tên đã cho đã tồn tại chưa, nếu chưa thì sẽ tạo mới.
    :return:Không trả về gì
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('Hàm tạo bảng ở interface A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Nạp dữ liệu từ file ratingsfilepath vào bảng ratingstablename.
    """

    con = openconnection
    cur = con.cursor()

    # Xóa bảng nếu đã tồn tại
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename + ";")

    # Tạo bảng với 3 cột chính
    cur.execute("CREATE TABLE " + ratingstablename + " (userid INTEGER, movieid INTEGER, rating FLOAT);")

    try:
        with open(ratingsfilepath, 'r') as f:
            # Tạo bảng tạm chứa tất cả các trường
            cur.execute("DROP TABLE IF EXISTS temp_ratings;")
            cur.execute("CREATE TABLE temp_ratings (userid INTEGER, extra1 CHAR, movieid INTEGER, extra2 CHAR, rating FLOAT, extra3 CHAR, timestamp BIGINT);")

            f.seek(0)  # Đưa con trỏ file về đầu
            cur.copy_from(f, 'temp_ratings', sep=':')

            # Chèn dữ liệu đã lọc vào bảng chính
            cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) SELECT userid, movieid, rating FROM temp_ratings;")

            # Xóa bảng tạm
            cur.execute("DROP TABLE temp_ratings;")

        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    
    connection = None
    cursor = None
    try:
        connection = openconnection
        cursor = connection.cursor()
        
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE tablename LIKE 'range_part%';
        """)
        old_tables = cursor.fetchall()
        for table_tuple in old_tables:
            table_name = table_tuple[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                partition_type TEXT,
                num_partitions INTEGER,
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
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)
            cursor.execute(f"DELETE FROM {table_name};")
            
            if i == 0:
                cursor.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating >= %s AND rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
            else:
                cursor.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating > %s AND rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
        
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if not (0 <= rating <= 5):
        raise Exception("Rating must be between 0 and 5")
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    connection = None
    cursor = None
    try:
        connection = openconnection
        cursor = connection.cursor()
        
        cursor.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
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
                cursor.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
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
            

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return

    cur = openconnection.cursor()
    
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Xoá các phân mảnh cũ (nếu có) và tạo phân mảnh mới
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i};")
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                userid INT,
                movieid INT,
                rating FLOAT
            );
        """)

    # Tạo metadata lưu trạng thái vòng tròn
    cur.execute("DROP TABLE IF EXISTS rrobin_metadata;")
    cur.execute("CREATE TABLE rrobin_metadata (partition_count INT, next_index INT);")
    cur.execute("INSERT INTO rrobin_metadata VALUES (%s, 0);", (numberofpartitions,))

    # Lấy toàn bộ dữ liệu từ bảng chính
    cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename};")
    rows = cur.fetchall()

    # Chèn dữ liệu vào các phân mảnh
    for i, row in enumerate(rows):
        target_partition = i % numberofpartitions
        cur.execute(f"INSERT INTO {RROBIN_TABLE_PREFIX}{target_partition} VALUES (%s, %s, %s);", row)

    openconnection.commit()
    cur.close()

    
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    # Chèn vào bảng Ratings
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    # Lấy thông tin metadata
    cur.execute("SELECT partition_count, next_index FROM rrobin_metadata;")
    partition_count, next_index = cur.fetchone()

    # Tính phân mảnh tiếp theo cần chèn
    target_partition = next_index % partition_count
    cur.execute(f"INSERT INTO rrobin_part{target_partition} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    # Cập nhật chỉ số vòng tròn
    cur.execute("UPDATE rrobin_metadata SET next_index = %s;", (next_index + 1,))

    openconnection.commit()
    cur.close()
