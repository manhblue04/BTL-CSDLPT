import traceback
import psycopg2

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# Các hàm thiết lập
def createdb(dbname):
    """
    Chúng ta tạo một DB bằng cách kết nối đến người dùng và cơ sở dữ liệu mặc định của Postgres
    Hàm đầu tiên kiểm tra xem cơ sở dữ liệu đã tồn tại cho một tên nhất định chưa, nếu chưa thì tạo mới.
    :return:None
    """
    # Kết nối đến cơ sở dữ liệu mặc định
    con = getopenconnection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Kiểm tra xem cơ sở dữ liệu với tên tương tự đã tồn tại chưa
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Tạo cơ sở dữ liệu
    else:
        print('Cơ sở dữ liệu có tên "{0}" đã tồn tại'.format(dbname))

    # Dọn dẹp
    cur.close()
    con.close()

def delete_db(dbname):
    con = getopenconnection(dbname = 'postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('drop database ' + dbname)
    cur.close()
    con.close()


def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


####### Hỗ trợ kiểm thử
def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Lấy số lượng hàng cho mỗi phân vùng
    :param ratingstablename:
    :param numberofpartitions:
    :param openconnection:
    :return:
    """
    cur = openconnection.cursor()
    countList = []
    interval = 5.0 / numberofpartitions
    cur.execute("select count(*) from {0} where rating >= {1} and rating <= {2}".format(ratingstablename,0, interval))
    countList.append(int(cur.fetchone()[0]))

    lowerbound = interval
    for i in range(1, numberofpartitions):
        cur.execute("select count(*) from {0} where rating > {1} and rating <= {2}".format(ratingstablename,
                                                                                          lowerbound,
                                                                                          lowerbound + interval))
        lowerbound += interval
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList


def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    '''
    Lấy số lượng hàng cho mỗi phân vùng
    :param ratingstablename:
    :param numberofpartitions:
    :param openconnection:
    :return:
    '''
    cur = openconnection.cursor()
    countList = []
    for i in range(0, numberofpartitions):
        cur.execute(
            "select count(*) from (select *, row_number() over () from {0}) as temp where (row_number-1)%{1}= {2}".format(
                ratingstablename, numberofpartitions, i))
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

# Các hàm hỗ trợ cho các hàm kiểm thử
def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
            prefix))
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:  raise Exception(
        'Phân vùng theo khoảng không được thực hiện đúng. Mong đợi {0} bảng nhưng tìm thấy {1} bảng'.format(
            expectedpartitions,
            count))


def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = []
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append('SELECT * FROM {0}{1}'.format(rangepartitiontableprefix, i))
    cur.execute('SELECT COUNT(*) FROM ({0}) AS T'.format(' UNION ALL '.join(selects)))
    count = int(cur.fetchone()[0])
    return count


def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            # Kiểm tra 1: Kiểm tra số lượng bảng được tạo, nếu 'n' không hợp lệ
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            # Kiểm tra 2: Kiểm tra số lượng bảng được tạo, nếu tất cả tham số đều đúng
            checkpartitioncount(cur, n, rangepartitiontableprefix)

            # Kiểm tra 3: Kiểm tra tính đầy đủ bằng cách sử dụng SQL UNION ALL
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count < ACTUAL_ROWS_IN_INPUT_FILE: raise Exception(
                "Tính đầy đủ của phân vùng thất bại. Mong đợi {0} hàng sau khi hợp nhất tất cả các bảng, nhưng tìm thấy {1} hàng".format(
                    ACTUAL_ROWS_IN_INPUT_FILE, count))

            # Kiểm tra 4: Kiểm tra tính rời rạc bằng cách sử dụng SQL UNION
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count > ACTUAL_ROWS_IN_INPUT_FILE: raise Exception(
                "Tính rời rạc của phân vùng thất bại. Mong đợi {0} hàng sau khi hợp nhất tất cả các bảng, nhưng tìm thấy {1} hàng".format(
                    ACTUAL_ROWS_IN_INPUT_FILE, count))

            # Kiểm tra 5: Kiểm tra tính tái tạo bằng cách sử dụng SQL UNION
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count != ACTUAL_ROWS_IN_INPUT_FILE: raise Exception(
                "Tính tái tạo của phân vùng thất bại. Mong đợi {0} hàng sau khi hợp nhất tất cả các bảng, nhưng tìm thấy {1} hàng".format(
                    ACTUAL_ROWS_IN_INPUT_FILE, count))


def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    with openconnection.cursor() as cur:
        cur.execute(
            'SELECT COUNT(*) FROM {0} WHERE {4} = {1} AND {5} = {2} AND {6} = {3}'.format(expectedtablename, userid,
                                                                                          itemid, rating,
                                                                                          USER_ID_COLNAME,
                                                                                          MOVIE_ID_COLNAME,
                                                                                          RATING_COLNAME))
        count = int(cur.fetchone()[0])
        if count != 1:  return False
        return True

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("select count(*) from {0}{1}".format(rangepartitiontableprefix, i))
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception("{0}{1} có {2} hàng trong khi số lượng đúng phải là {3}".format(
                rangepartitiontableprefix, i, count, countList[i]
            ))

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("select count(*) from {0}{1}".format(roundrobinpartitiontableprefix, i))
        count = cur.fetchone()[0]
        if count != countList[i]:
            raise Exception("{0}{1} có {2} hàng trong khi số lượng đúng phải là {3}".format(
                roundrobinpartitiontableprefix, i, count, countList[i]
            ))

# ##########

def testloadratings(MyAssignment, ratingstablename, filepath, openconnection, rowsininpfile):
    """
    Kiểm tra hàm load ratings
    :param ratingstablename: Tham số cho hàm cần kiểm tra
    :param filepath: Tham số cho hàm cần kiểm tra
    :param openconnection: Tham số cho hàm cần kiểm tra
    :param rowsininpfile: Số lượng dòng trong file đầu vào để kiểm tra
    :return: Ném ra ngoại lệ nếu bất kỳ test nào thất bại
    """
    try:
        MyAssignment.loadratings(ratingstablename,filepath,openconnection)
        # Kiểm tra 1: Đếm số lượng dòng đã chèn vào
        with openconnection.cursor() as cur:
            cur.execute('SELECT COUNT(*) from {0}'.format(ratingstablename))
            count = int(cur.fetchone()[0])
            if count != rowsininpfile:
                raise Exception(
                    'Mong đợi {0} hàng, nhưng có {1} hàng trong bảng \'{2}\''.format(rowsininpfile, count, ratingstablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]


def testrangepartition(MyAssignment, ratingstablename, n, openconnection, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Kiểm tra hàm phân vùng theo khoảng cho tính đầy đủ, tính rời rạc và tính tái tạo
    :param ratingstablename: Tham số cho hàm cần kiểm tra
    :param n: Tham số cho hàm cần kiểm tra
    :param openconnection: Tham số cho hàm cần kiểm tra
    :param partitionstartindex: Chỉ ra cách đánh số tên bảng. Bắt đầu từ rangepart1, 2... hay rangepart0, 1, 2...
    :return: Ném ra ngoại lệ nếu bất kỳ test nào thất bại
    """

    try:
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
        testrangeandrobinpartitioning(n, openconnection, RANGE_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRangePartition(ratingstablename, n, openconnection, RANGE_TABLE_PREFIX)
        return [True, None]
    except Exception as e:
        traceback.print_exc()
        return [False, e]


def testroundrobinpartition(MyAssignment, ratingstablename, numberofpartitions, openconnection,
                            partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    """
    Kiểm tra phân vùng round robin cho tính đầy đủ, tính rời rạc và tính tái tạo
    :param ratingstablename: Tham số cho hàm cần kiểm tra
    :param numberofpartitions: Tham số cho hàm cần kiểm tra
    :param openconnection: Tham số cho hàm cần kiểm tra
    :param partitionstartindex: Hàm này giả định rằng các bảng của bạn được đặt tên theo thứ tự. Ví dụ: robinpart1, robinpart2...
    :return: Ném ra ngoại lệ nếu bất kỳ test nào thất bại
    """
    try:
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
        testrangeandrobinpartitioning(numberofpartitions, openconnection, RROBIN_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRoundrobinPartition(ratingstablename, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testroundrobininsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    """
    Kiểm tra hàm chèn roundrobin bằng cách kiểm tra xem bản ghi có được chèn vào bảng mong đợi không
    :param ratingstablename: Tham số cho hàm cần kiểm tra
    :param userid: Tham số cho hàm cần kiểm tra
    :param itemid: Tham số cho hàm cần kiểm tra
    :param rating: Tham số cho hàm cần kiểm tra
    :param openconnection: Tham số cho hàm cần kiểm tra
    :param expectedtableindex: Bảng mong đợi mà bản ghi phải được lưu vào
    :return: Ném ra ngoại lệ nếu bất kỳ test nào thất bại
    """
    try:
        expectedtablename = RROBIN_TABLE_PREFIX + expectedtableindex
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Chèn round robin thất bại! Không tìm thấy bản ghi ({0}, {1}, {2}) trong bảng {3}'.format(userid, itemid, rating,
                                                                                                    expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]


def testrangeinsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    """
    Kiểm tra hàm chèn theo khoảng bằng cách kiểm tra xem bản ghi có được chèn vào bảng mong đợi không
    :param ratingstablename: Tham số cho hàm cần kiểm tra
    :param userid: Tham số cho hàm cần kiểm tra
    :param itemid: Tham số cho hàm cần kiểm tra
    :param rating: Tham số cho hàm cần kiểm tra
    :param openconnection: Tham số cho hàm cần kiểm tra
    :param expectedtableindex: Bảng mong đợi mà bản ghi phải được lưu vào
    :return: Ném ra ngoại lệ nếu bất kỳ test nào thất bại
    """
    try:
        expectedtablename = RANGE_TABLE_PREFIX + expectedtableindex
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Chèn theo khoảng thất bại! Không tìm thấy bản ghi ({0}, {1}, {2}) trong bảng {3}'.format(userid, itemid, rating,
                                                                                              expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]