import json
import requests
from datetime import datetime
from pipeline import dataSave
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


def etherscan1(addr):
    url = 'http://api.etherscan.io/api?module=account&action=txlist&address={}&startblock=0&endblock=99999999&sort=asc&apikey=YourApiKeyToken'.format(
        addr)
    try:
        response = requests.get(url)
    except Exception as e:
        print('get_info', e)
    else:
        if response:
            Json = json.loads(response.text)
            result = Json['result']
            lastBlockId = None
            for r in result:
                Array = [
                    r['blockNumber'],
                    int(r['timeStamp']),
                    r['hash'],
                    r['nonce'],
                    r['blockHash'],
                    int(r['transactionIndex']),
                    r['from'],
                    r['to'],
                    int(r['value']),
                    int(r['gas']),
                    int(r['gasPrice']),
                    r['isError'],
                    int(r['txreceipt_status']),
                    r['input'],
                    r['contractAddress'],
                    int(r['cumulativeGasUsed']),
                    int(r['gasUsed']),
                    int(r['confirmations']),
                    'ETH'
                ]
                lastBlockId = r['blockNumber']
                sql = '''INSERT INTO `datc_wallet_transactions`(`blockNumber`, `timeStamp`, `hash`, `nonce`, `blockHash`, `transactionIndex`, `from`, `to`, `value`, `gas`, `gasPrice`, `isError`, `txreceipt_status`, `input`, `contractAddress`, `cumulativeGasUsed`, `gasUsed`, `confirmations`, `trans_type`)
                 VALUES (?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?)'''
                print(sql)
                print(Array)
                try:
                    with mysql() as cursor:
                        cursor.execute(sql, Array)
                except EOFError as e:
                    print('dataSave EOFError', e)
                except Exception as e:
                    print('dataSave Exception', e)
            print("=========")
            # if lastBlockId:
            #     Dict2 = {}
            #     now = datetime.now()
            #     Dict2['last_block_id'] = lastBlockId
            #     Dict2['created_at'] = now.strftime('%Y-%m-%d %H:%M:%S')
            #     Dict2['updated_at'] = now.strftime('%Y-%m-%d %H:%M:%S')
            #     try:
            #         dataSave().SavaInfo(Dict2, 'datc_wallet_trans_settings')
            #     except EOFError as e:
            #         print('dataSave EOFError', e)
            #     except Exception as e:
            #         print('dataSave Exception', e)


if __name__ == '__main__':
    # 执行sql
    with mysql() as cursor:
        print(cursor)
        sql = "select * from datc_wallet_accounts where address IS NOT NULL and status = 'enabled'"
        row_count = cursor.execute(sql)
        row = cursor.fetchall()
        #for account in row:
        #etherscan1(account['address'])
        etherscan1('0xa2a3F71fC4B6Ce9897DDa740c343718bBBcCB11A')
