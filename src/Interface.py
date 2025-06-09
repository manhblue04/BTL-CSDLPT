#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import io

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='123456', dbname='postgres'):
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
        
        RANGE_TABLE_PREFIX = 'range_part'

        # Xóa các bảng phân vùng cũ
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE tablename LIKE 'range_part%';
        """)
        old_tables = cursor.fetchall()
        for table_tuple in old_tables:
            table_name = table_tuple[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        # Tạo bảng metadata để lưu thông tin phân vùng
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                partition_type TEXT,
                num_partitions INTEGER,
                range_boundaries TEXT
            );
        """)
        
        cursor.execute("DELETE FROM metadata WHERE partition_type = 'range';")
        
        # Tính toán ranh giới cho các phân vùng
        max_rating = 5.0
        min_rating = 0.0
        delta = (max_rating - min_rating) / numberofpartitions
        boundaries = [min_rating + i * delta for i in range(numberofpartitions + 1)]
        
        # Làm tròn lên 2 chữ số thập phân
        boundaries = [round(b * 100 + 0.5) / 100 for b in boundaries]
        boundaries_str = ",".join(str(b) for b in boundaries)

        # Lưu thông tin phân vùng vào metadata
        cursor.execute("""
            INSERT INTO metadata (partition_type, num_partitions, range_boundaries)
            VALUES (%s, %s, %s);
        """, ('range', numberofpartitions, boundaries_str))
        
        # Tạo và phân phối dữ liệu cho các phân vùng
        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
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
        raise Exception("Rating must be between 0 and 5.")
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    connection = None
    cursor = None
    try:
        connection = openconnection
        cursor = connection.cursor()

        RANGE_TABLE_PREFIX = 'range_part'

        # Kiểm tra xem dữ liệu đã tồn tại trong bảng gốc chưa
        cursor.execute(f"""
            SELECT 1 FROM {ratingstablename}
            WHERE userid = %s AND movieid = %s AND rating = %s;
        """, (userid, itemid, rating))

        if cursor.fetchone():
            print("Dữ liệu đã tồn tại trong Ratings")
            return
        
        # Chèn dữ liệu vào bảng chính
        cursor.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        # Lấy thông tin phân vùng từ metadata
        cursor.execute("SELECT num_partitions FROM metadata WHERE partition_type = 'range';")
        num_partitions = cursor.fetchone()[0]
        
        # Tính trực tiếp phân mảnh dựa trên rating
        partition_size = round(5.0 / num_partitions, 2)
        partition_index = int(rating / partition_size)
        if rating % partition_size == 0 and rating != 0:
            partition_index -= 1
        
        # Chèn vào phân mảnh tương ứng
        table_name = f"{RANGE_TABLE_PREFIX}{partition_index}"
        cursor.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
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

    try:
        cur = openconnection.cursor()
        
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Xóa và tạo lại các bảng phân mảnh
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i};")
            cur.execute(f"""
                CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                    userid INT,
                    movieid INT,
                    rating FLOAT
                );
            """)

        # Tạo lại bảng metadata lưu tổng số phân mảnh và chỉ số tiếp theo của bảng cần chèn
        cur.execute("DROP TABLE IF EXISTS rrobin_metadata;")
        cur.execute("CREATE TABLE rrobin_metadata (partition_count INT, next_index INT);")
        cur.execute("INSERT INTO rrobin_metadata VALUES (%s, 0);", (numberofpartitions,))

        # Lấy dữ liệu từ bảng chính
        cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename};")
        rows = cur.fetchall()

        # Chuẩn bị buffer dạng file ảo theo từng phân mảnh, mỗi buffer sẽ chứa dữ liệu cho một phân mảnh
        buffers = [io.StringIO() for _ in range(numberofpartitions)]
        
        for i, row in enumerate(rows):
            partition_index = i % numberofpartitions 
            line = f"{row[0]}\t{row[1]}\t{row[2]}\n" #lấy dữ liệu cột userid, movieid, rating và phân tách bằng tab
            buffers[partition_index].write(line)

        for i in range(numberofpartitions):
            # Đặt lại vị trí con trỏ đầu mỗi buffer
            buffers[i].seek(0)
            cur.copy_from(buffers[i], f"{RROBIN_TABLE_PREFIX}{i}", columns=("userid", "movieid", "rating"))

        openconnection.commit()

    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        if cur:
            cur.close()
    
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if not (0 <= rating <= 5):
        raise Exception("Rating must be between 0 and 5.")
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    try:
        cur = openconnection.cursor()

         # Kiểm tra xem dữ liệu đã tồn tại trong bảng gốc chưa
        cur.execute(f"""
            SELECT 1 FROM {ratingstablename}
            WHERE userid = %s AND movieid = %s AND rating = %s;
        """, (userid, itemid, rating))
        
        if cur.fetchone():
            print("Dữ liệu đã tồn tại trong Ratings")
            return
            
        # Chèn vào bảng Ratings
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

        # Lấy thông tin metadata
        cur.execute("SELECT partition_count, next_index FROM rrobin_metadata;")
        partition_count, next_index = cur.fetchone()

        RROBIN_TABLE_PREFIX = 'rrobin_part'
        
        # Tính phân mảnh tiếp theo cần chèn
        target_partition = next_index % partition_count
        cur.execute(f"INSERT INTO {RROBIN_TABLE_PREFIX}{target_partition} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

        # Cập nhật chỉ số vòng tròn
        cur.execute("UPDATE rrobin_metadata SET next_index = %s;", (next_index + 1,))

        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        if cur:
            cur.close()