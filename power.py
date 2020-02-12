#!/usr/bin/python3
""","""
# *********************************************************************
try:  # Genral python imports
    import datetime
    import time
    import threading
    from datetime import datetime
    import timestamp
    import sys
    import os
    import concurrent.futures
except ImportError:
    print("Cannot import datetime, time, threading, timestamp is it installed?\nExiting...")
# *********************************************************************
try:  # matplotlib
    import matplotlib 
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError:
    print("Cannot import matplotlib, is it installed?\nExiting...")
    sys.exit
# *********************************************************************
try:  # pymysql
    import pymysql
except ImportError:
    print("Cannot import pymysql, is it installed?\nExiting...")
    sys.exit
# *********************************************************************
try:  # BlockingScheduler
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    print("Cannot import apscheduler, is it installed?\nExiting...")
    sys.exit
# *********************************************************************
try:  # pymysqlpool
    from pymysqlpool.pool import Pool
except ImportError:
    print("Cannot import pymysqlpool, pip install pymysql-pooling is it installed?\nExiting...")
    sys.exit
# *********************************************************************
try:  # pylab
    import pylab
except ImportError:
    print("Cannot import pylab, is it installed?\nExiting...")
    sys.exit
# *********************************************************************
try:  # pzem
    import pzem
except ImportError:
    print("Cannot import pzem, is it installed?\nExiting...")
    sys.exit
# *********************************************************************
try:  # ConfigParser
    import configparser
    from configparser import ConfigParser
except ImportError:
    print("Cannot import ConfigParser, is it installed?\nExiting...")
    sys.exit
# *********************************************************************
# end imports
# *********************************************************************

# *********************************************************************
def create_conn():
    try:
    ##https://github.com/egmkang/PyMySQLPool
    ##https://pypi.org/project/pymysql-pooling/
    #    cursor = pymysql.cursors.DictCursor
        pool = Pool(host=mysql_host, \
                               port=int(mysql_port), \
                               user=mysql_user, \
                               password=mysql_password, \
                               database=mysql_db, \
                               charset='utf8')
        pool.init()
        connection = pool.get_conn()
        cursor = connection.cursor() 
        cursor.execute(fn_sql)
        result = cursor.fetchall()
        return result
 
    except pymysql.err.OperationalError as err:
        print('create_conn ', err)
        os._exit(1)
    except:
        print('MySQL Error create_conn')
        os._exit(1)

# *********************************************************************
def sec_job():
    try:
        if Debug == True:
            print("Save Images")
        sql = "SELECT DATE_FORMAT(nowdatetime, '%Y-%m-%d %H:%i:%s'), *record*  \
        FROM powerdata WHERE nowdatetime BETWEEN NOW() - INTERVAL 5 MINUTE AND NOW()"
        titlest = "Last 5 Minutes  *record*"
        filname = "Minute *record*.png"
        myslread(sql, titlest, filname)
    except:
        print("Error in sec_job ",  sql)
        sys.exit()

# *********************************************************************
def hr_job():
    try:
        if Debug == True:
            print("Save Images")
        sql = "SELECT DATE_FORMAT(nowdatetime, '%Y-%m-%d %H:%i:%s'), *record*  \
        FROM powerdata WHERE nowdatetime BETWEEN NOW() - INTERVAL 1 Hour AND NOW()"
        titlest = "Last 1 Hour *record*"
        filname = "Hour *record*.png"
        myslread(sql, titlest, filname)
    except:
        print("Error in hr_job ",  sql)
        sys.exit()

# *********************************************************************
def day_runing():
    try:
        if Debug == True:
            print("Save Images")
        sql = "SELECT DATE_FORMAT(nowdatetime, '%Y-%m-%d %H:%i:%s'), *record*  \
        FROM powerdata WHERE nowdatetime BETWEEN NOW() - INTERVAL 1 Day AND NOW()"
        titlest = "Last Day *record*"
        filename = "Day *record*.png"
        myslread(sql, titlest, filename)
    except:
        print("Error in day_running ",  sql)
        os._exit(1)

# *********************************************************************
def day_job():
    try:
        if Debug == True:
            print("Save Images")
        sql = "SELECT DATE_FORMAT(nowdatetime, '%Y-%m-%d %H:%i:%s'), *record* \
        FROM powerdata WHERE nowdatetime BETWEEN NOW() - INTERVAL 1 Day AND NOW()"
        timerstr = time.strftime("%Y%m%d")
        titlest = timerstr + " Day *record*"
        filename = timerstr + "*record*.png"
        myslread(sql, titlest, filename)
    except:
        print("Error in day_job ",  sql)
        sys.exit()

# *********************************************************************
def myslread(fnin_sql, fnin_title, fnin_filename):
    try:
#        conn = pool.get()
#        cursor = conn.cursor()
        for record in graphArray:
            fn_sql = (fnin_sql.replace("*record*", record))
            fn_title = (fnin_title.replace("*record*", record))
            fn_filename = (fnin_filename.replace("*record*", record))
            if Debug == False:
                print("********************")
                print("SQL =",fn_sql)
                print("Title",fn_title)
                print("FileName =",fn_filename)
                print("********************")
            # cursor = db.cursor()
            connection = pool.get_conn()
            cursor = connection.cursor() 
            cursor.execute(fn_sql)
            result = cursor.fetchall()
            t = []
            s = []
            s.clear()
            t.clear()
            fig, ax = plt.subplots()
            try:
                for record in result:
                    t.append(record[0])
                    s.append(record[1])
                    # figure()
                    # plot(t, s, 'r.-')
                    plt.title(fn_title)
                    plt.grid(True)
                    # plt = gcf()
                    # DPI = plt.get_dpi()
                    # plt.savefig(fn_filename , dpi = (160))
                    plt.step(t, s, linewidth=2, where='mid')
                    plt.savefig(fn_filename)  # write image to file
                    plt.cla()
                plt.close(fig)
            except:
                print("Error in image make ",  fn_sql)
                sys.exit()
        #cursor.close()
        #conn.close()
    # https://stackoverflow.com/questions/21884271/warning-about-too-many-open-figures
    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        print("MySQL Error myslread")
        if Debug == False:
            print("SQL=",fnin_sql)
            print("Title =",fnin_title)
            print("FileName =", fnin_filename)
        #cursor.close()
        conn.close()
        sys.exit

# *********************************************************************
def main_function():
#    conn = pool.get()
#    cursor = conn.cursor()
    maxdevice = max(slaveAddresses)
    for s in slaveAddresses:
        try:
            i = int(s)
            device[i] = pzem.pzem(serialPort, i)
            if Debug == True:
                device[i].debug = True
        except Exception as e:
            print("Device:" + str(i) + ", Setup Error:" + str(e))
    try:
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
      
        for i in device:
            data[i] = False
            try:
                data[i] = device[i].getData()
            except IOError as e:
                print("Device:" + str(device[i].address) + ", IOError:" + str(e))
            except ValueError as e:
                print("Device:" + str(device[i].address) + ", ValueError:" + str(e))
            except TypeError as e:
                print("Device:" + str(device[i].address) + ", ValueError:" + str(e))
            # end try
            if data[i]:
                if Debug == True:
                    print(' '.join(str(v) for v in [ts.previous, device[i].address] + data[i]))
                    print(' '.join(str(v) for v in data[i]))
                # end Debug
                if i == 1:
                    adata = data[i]
                    GridVoltage = adata[0]
                    GridCurrent = adata[1]
                    GridPower = adata[2]
                    GridEnergy = adata[3]
                    GridFrequency = adata[4]  
                    GridPowerFactor = adata[5]
                    GridAlarm = adata[6]
                    if Debug == True:
                        print(GridVoltage)
                        print(GridCurrent)
                        print(GridPower)
                        print(GridEnergy)
                        print(GridFrequency)
                        print(GridPowerFactor)
                        print(GridAlarm)
                    # End Debug
                    if maxdevice == 1:
                        sql = "INSERT INTO powerdata(GridCurrent, GridVoltage, GridPower,GridEnergy, GridFrequency, GridPowerFactor, GridAlarm, nowdatetime) values(%s, %s, %s, %s, %s, %s, %s, %s)"
                        sdata = GridCurrent, GridVoltage, GridPower, GridEnergy, GridFrequency, GridPowerFactor, GridAlarm, formatted_date
                        if Debug == True:
                            print(sql, str(sdata))
                        try:
                            # Execute the SQL command
                            connection = pool.get_conn()
                            cursor = connection.cursor() 
                            cursor.execute(sql, sdata)
                        except pymysql.Error as e:
                            print("could not close execute pymysql %d: %s" % (e.args[0], e.args[1]))
                            sys.exit
                        conn.commit()
                # end if 1
                if i == 2:
                    adata = data[i]
                    SolarVoltage = adata[0]
                    SolarCurrent = adata[1]
                    SolarPower = adata[2]
                    SolarEnergy = adata[3]
                    SolarFrequency = adata[4]
                    SolarPowerFactor = adata[5]
                    SolarAlarm = adata[6]
                    if Debug == 1:
                        print(SolarVoltage)
                        print(SolarCurrent)
                        print(SolarPower)
                        print(SolarEnergy)
                        print(SolarFrequency)
                        print(SolarPowerFactor)
                        print(SolarAlarm)
                    # end Debug
                    if maxdevice == 2:
                        sql = "INSERT INTO powerdata(GridCurrent, GridVoltage, GridPower,GridEnergy, GridFrequency, GridPowerFactor, GridAlarm, SolarCurrent, SolarVoltage, SolarPower, SolarEnergy, SolarFrequency, SolarPowerFactor, SolarAlarm,  nowdatetime) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        sdata = GridCurrent, GridVoltage, GridPower, GridEnergy, GridFrequency, GridPowerFactor, GridAlarm, SolarCurrent, SolarVoltage, SolarPower, SolarEnergy, SolarFrequency, SolarPowerFactor, SolarAlarm, formatted_date 
                        if Debug == True:
                            print(sql, str(sdata))
                        try:
                            # Execute the SQL command
                            cursor.execute(sql, sdata)
                        except pymysql.Error as e:
                            print("could not close execute pymysql %d: %s" % (e.args[0], e.args[1]))
                            sys.exit
                        conn.commit()
                    # end if 2    
            else:
                if Debug == True:
                    print("No data found for device:" + str(device[i].address))
            # end if data[i]
        # end for i
        #cursor.close()
        #conn.close()
    except KeyboardInterrupt:
        cursor.close()
        conn.close()
        print("Exiting...")  # Gracefully exit on "CTRL-C"
        sys.exit
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


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
# dmesg | grep tty       list Serial linux command
# *********************************************************************
# End read config
# *********************************************************************
#pool = pymysqlpool.Pool(create_instance=create_conn)
graphArray = []
device = {}
data = {}
for s in slaveAddresses:
    try:
        i = int(s)
        device[i] = pzem.pzem(serialPort, i)
        if Debug == True:
            device[i].debug = True
    except Exception as e:
        print("Device:" + str(i) + ", Setup Error:" + str(e))
for i in device:
    if i == 1:
        graphArray.append("GridVoltage")
        graphArray.append("GridCurrent")
        graphArray.append("GridPower")
        graphArray.append("GridEnergy")
        graphArray.append("GridFrequency")
        graphArray.append("GridPowerFactor")
        graphArray.append("GridAlarm")
    if i == 2:
        graphArray.append("SolarVoltage")
        graphArray.append("SolarCurrent")
        graphArray.append("SolarPower")
        graphArray.append("SolarEnergy")
        graphArray.append("SolarFrequency")
        graphArray.append("SolarPowerFactor")
        graphArray.append("SolarAlarm")
ts = timestamp.interval(interval)

#conn = pool.get()
#cursor = conn.cursor()
scheduler = BlockingScheduler()
scheduler.add_job(main_function, 'cron', second = '*/30')
scheduler.add_job(sec_job,       'cron', minute = '*/1')
scheduler.add_job(day_runing,    'cron', minute = '*/5')  # ever 5 min all day
scheduler.add_job(hr_job,        'cron', hour   = '*/1')  # Each Hr
scheduler.add_job(day_job,       'cron', day    = '*/1')  # Each Day
scheduler.print_jobs()
scheduler.start()


#https://github.com/agronholm/apscheduler/issues/346


# *********************************************************************
