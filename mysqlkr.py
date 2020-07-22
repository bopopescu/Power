import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling# *********************************************************************
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
    subordinateAddresses = [int(x) for x in config.get('serial', 'addresses').split(',')]
except configparser.NoOptionError as err:
    print('Error ', err)
    sys.exit
except configparser.MissingSectionHeaderError as err:
    print('ERROR: Missing Section ', err)
    sys.exit
except configparser.ParsingError as err:
    print('ERROR:', err)
    sys.exit

try:
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                                                  pool_size=5,
                                                                  pool_reset_session=True,
                                                                  host=mysql_host,
                                                                  database=mysql_db,
                                                                  user=mysql_user,
                                                                  password=mysql_password)

    print ("Printing connection pool properties ")
    print("Connection Pool Name - ", connection_pool.pool_name)
    print("Connection Pool Size - ", connection_pool.pool_size)

    # Get connection object from a pool
    connection_object = connection_pool.get_connection()


    if connection_object.is_connected():
       db_Info = connection_object.get_server_info()
       print("Connected to MySQL database using connection pool ... MySQL Server version on ",db_Info)

       cursor = connection_object.cursor()
       cursor.execute("select database();")
       record = cursor.fetchone()
       print ("Your connected to - ", record)

except Error as e :
    print ("Error while connecting to MySQL using Connection pool ", e)
finally:
    #closing database connection.
    if(connection_object.is_connected()):
        cursor.close()
        connection_object.close()
        print("MySQL connection is closed")