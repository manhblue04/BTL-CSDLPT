# import psycopg2
import mysql.connector


# Hàm này tạo và trả về một kết nối đến cơ sở dữ liệu MySQL
# với các thông tin đăng nhập mặc định.
def getopenconnection(user='root', password='123456', dbname='csdlpt'):
    return mysql.connector.connect(
        host='localhost',
        user=user,
        password=password,
        database=dbname
    )

# Hàm này tạo một database MySQL nếu nó chưa tồn tại.
def create_db(dbname):
    con = mysql.connector.connect(
        host='localhost',
        user='root',
        password='123456'
    )
    cur = con.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}`;")
    cur.close()
    con.close()

# Hàm này tải dữ liệu từ file ratings vào một bảng trong cơ sở dữ liệu.
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    # Tạo database nếu chưa có
    create_db(openconnection.database)
    con = openconnection
    cur = con.cursor()
    
    # Tạo bảng nếu chưa tồn tại
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INT,
            movieid INT,
            rating FLOAT
        );
    """)
    
    # Đọc file và chèn dữ liệu
    with open(ratingsfilepath, 'r') as file:
        for line in file:
            parts = line.strip().split('::')
            if len(parts) >= 3:
                userid = int(parts[0])
                movieid = int(parts[1])
                rating = float(parts[2])
                cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))
    cur.close()
    con.commit()



def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(0, numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        if i == 0:
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minRange) + " and rating <= " + str(maxRange) + ";")
        else:
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minRange) + " and rating <= " + str(maxRange) + ";")
    cur.close()
    con.commit()
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(i) + ";")
    cur.close()
    con.commit()
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows-1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()
def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
