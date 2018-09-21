import pymysql
import contextlib


# 定义上下文管理器，连接后自动关闭连接
@contextlib.contextmanager
def mysql(host='127.0.0.1', port=3306, user='root', passwd='root', db='Wallent', charset='utf8'):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        yield cursor
    finally:
        conn.commit()
        cursor.close()
        conn.close()


# 执行sql
with mysql() as cursor:
    print(cursor)
    sql = "select * from datc_wallet_accounts where address IS NOT NULL and status = 'enabled'"
    row_count = cursor.execute(sql)
    row = cursor.fetchall()
    for account in row:
       print(account['address'])
