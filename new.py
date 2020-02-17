from multiprocessing import Pool
from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import connect
import os

pool = None

def init():
    global pool
    print("PID %d: initializing pool..." % os.getpid())
    pool = MySQLConnectionPool(...)

def do_work(q):
    con = pool.get_connection()
    print("PID %d: using connection %s" % (os.getpid(), con))
    c = con.cursor()
    c.execute(q)
    res = c.fetchall()
    con.close()
    return res

def main():
    p = Pool(initializer=init)
    for res in p.map(do_work, ['select * from test']*8):
        print(res)
    p.close()
    p.join()

if __name__ == '__main__':
    main()