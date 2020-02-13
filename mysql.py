
#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import mysql.connector.pooling
# *********************************************************************
try:  # ConfigParser
    import configparser
    from configparser import ConfigParser
except ImportError:
    print("Cannot import ConfigParser, is it installed?\nExiting...")
    sys.exit
# *********************************************************************

try:  # ConfigParser
    config = ConfigParser()
    config.read('../config.ini')
    mysql_host = config.get('mysql', 'host')
    mysql_db = config.get('mysql', 'database')
    mysql_user = config.get('mysql', 'user')
    mysql_password = config.get('mysql', 'password')
    mysql_port = config.get('mysql', 'port')
    serialPort = config.get('serial', 'port')
    Debug = config.getboolean('debug', 'debug')
    interval = int(config.get('timeing', 'interval'))
    slaveAddresses = [int(x) for x in config.get('serial', 'addresses').split(',')]
except configparser.NoOptionError as err:
    print('Error ', err)
    sys.exit
except configparser.MissingSectionHeaderError as err:
    print('ERROR: Missing Section ', err)
    sys.exit
except configparser.ParsingError as err:
    print('ERROR:', err)
    sys.exit

dbconfig = {
    "host":mysql_host,
    "port":mysql_port,
    "user":mysql_user,
    "password":mysql_password,
    "database":mysql_db,
}
##https://stackoverflow.com/questions/24374058/accessing-a-mysql-connection-pool-from-python-multiprocessing
class MySQLPool(object):
    """
    create a pool when connect mysql, which will decrease the time spent in 
    request connection, create connection and close connection.
    """
    def __init__(self,  pool_name="mypool", pool_size=3):
           
      
        res = {}
        self._host = mysql_host
        self._port = mysql_port
        self._user = mysql_user
        self._password = mysql_password
        self._database = mysql_db

        res["host"] = self._host
        res["port"] = self._port
        res["user"] = self._user
        res["password"] = self._password
        res["database"] = self._database
        self.dbconfig = res
        self.pool = self.create_pool(pool_name=pool_name, pool_size=pool_size)

    def create_pool(self, pool_name="mypool", pool_size=3):
        """
        Create a connection pool, after created, the request of connecting 
        MySQL could get a connection from this pool instead of request to 
        create a connection.
        :param pool_name: the name of pool, default is "mypool"
        :param pool_size: the size of pool, default is 3
        :return: connection pool
        """
        pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            pool_reset_session=True,
            **self.dbconfig)
        return pool

    def close(self, conn, cursor):
        """
        A method used to close connection of mysql.
        :param conn: 
        :param cursor: 
        :return: 
        """
        cursor.close()
        conn.close()

    def execute(self, sql, args=None, commit=False):
        """
        Execute a sql, it could be with args and with out args. The usage is 
        similar with execute() function in module pymysql.
        :param sql: sql clause
        :param args: args need by sql clause
        :param commit: whether to commit
        :return: if commit, return None, else, return result
        """
        # get connection form connection pool instead of create one.
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)
        if commit is True:
            conn.commit()
            self.close(conn, cursor)
            return None
        else:
            res = cursor.fetchall()
            self.close(conn, cursor)
            return res

    def executemany(self, sql, args, commit=False):
        """
        Execute with many args. Similar with executemany() function in pymysql.
        args should be a sequence.
        :param sql: sql clause
        :param args: args
        :param commit: commit or not.
        :return: if commit, return None, else, return result
        """
        # get connection form connection pool instead of create one.
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        cursor.executemany(sql, args)
        if commit is True:
            conn.commit()
            self.close(conn, cursor)
            return None
        else:
            res = cursor.fetchall()
            self.close(conn, cursor)
            return res


if __name__ == "__main__":
    mysql_pool = MySQLPool(**dbconfig)
    sql = "select * from store WHERE create_time < '2017-06-02'"
    p = Pool()
    for i in range(5):
        p.apply_async(mysql_pool.execute, args=(sql,))