import pymysql
import sys


class dataSave:
    def __init__(self):
        self.conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="root", db="Wallent",
                                    charset="utf8")
        self.cursor = self.conn.cursor()


    def SavaInfo(self,DictInfo,tablename):
        """
        判断item的类型，并作相应的处理，再入数据库
        :param DictInfo:
        :param tablename:
        :return:
        """
        try:
            cols = ','.join(DictInfo.keys())
            values = '","'.join(DictInfo.values())
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, '"' + values + '"')
            print(sql)
            result = self.cursor.execute(sql)
            insert_id = self.conn.insert_id()

            # 判断是否执行成功
            if result:
                print("插入成功:%s" % insert_id)
            else:
                print("插入为NULL")
        except pymysql.Error as e:
            print(e)
            # 发生错误时回滚
            self.conn.rollback()
            # 主键唯一，无法插入
            if "key 'PRIMARY'" in e.args[1]:
                print("数据已存在，未插入数据")
            else:
                print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        finally:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()



