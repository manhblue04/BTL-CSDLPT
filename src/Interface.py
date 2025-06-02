#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

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


# def rangepartition(ratingstablename, numberofpartitions, openconnection):
#     """
#     Function to create partitions of main table based on range of ratings.
#     """
#     con = openconnection
#     cur = con.cursor()
#     delta = 5 / numberofpartitions
#     RANGE_TABLE_PREFIX = 'range_part'
#     for i in range(0, numberofpartitions):
#         minRange = i * delta
#         maxRange = minRange + delta
#         table_name = RANGE_TABLE_PREFIX + str(i)
#         cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
#         if i == 0:
#             cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minRange) + " and rating <= " + str(maxRange) + ";")
#         else:
#             cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minRange) + " and rating <= " + str(maxRange) + ";")
#     cur.close()
#     con.commit()

# def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
#     """
#     Function to create partitions of main table using round robin approach.
#     """
#     con = openconnection
#     cur = con.cursor()
#     RROBIN_TABLE_PREFIX = 'rrobin_part'
#     for i in range(0, numberofpartitions):
#         table_name = RROBIN_TABLE_PREFIX + str(i)
#         cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
#         cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(i) + ";")
#     cur.close()
#     con.commit()

# def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
#     """
#     Function to insert a new row into the main table and specific partition based on round robin
#     approach.
#     """
#     con = openconnection
#     cur = con.cursor()
#     RROBIN_TABLE_PREFIX = 'rrobin_part'
#     cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
#     cur.execute("select count(*) from " + ratingstablename + ";");
#     total_rows = (cur.fetchall())[0][0]
#     numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
#     index = (total_rows-1) % numberofpartitions
#     table_name = RROBIN_TABLE_PREFIX + str(index)
#     cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
#     cur.close()
#     con.commit()

# def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
#     """
#     Function to insert a new row into the main table and specific partition based on range rating.
#     """
#     con = openconnection
#     cur = con.cursor()
#     RANGE_TABLE_PREFIX = 'range_part'
#     numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
#     delta = 5 / numberofpartitions
#     index = int(rating / delta)
#     if rating % delta == 0 and index != 0:
#         index = index - 1
#     table_name = RANGE_TABLE_PREFIX + str(index)
#     cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
#     cur.close()
#     con.commit()

# def count_partitions(prefix, openconnection):
#     """
#     Function to count the number of tables which have the @prefix in their name somewhere.
#     """
#     con = openconnection
#     cur = con.cursor()
#     cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
#     count = cur.fetchone()[0]
#     cur.close()

#     return count