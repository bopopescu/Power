#!/usr/bin/env python

import psycopg2
from config import config
from psycopg2.pool import ThreadedConnectionPool
from multiprocessing import Process
import time
import threading
from multiprocessing import Queue

data_queque = Queue()  # reader reads data from queue

SELECT_QUERY = 'Select something from some_table limit %s offset %s ';

INSERT_QUERY = "Insert INTO sometable (col1, col2, col3) values "

# writer write data to queue
class PsqlMultiThreadExample(object):
    _select_conn_count = 10;
    _insert_conn_count = 10;
    _insert_conn_pool = None;
    _select_conn_pool = None;

    def __init__(self):
        self = self;

    def postgres_connection(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def check_connection(self):
        """ Checking the postgres database connection"""
        conn = None;
        try:
            conn = PsqlMultiThreadExample._select_conn_pool.getconn()

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def create_connection_pool(self):
        """ Create the thread safe threaded postgres connection pool"""

        # calculate the max and min connection required
        max_conn = PsqlMultiThreadExample._insert_conn_count + PsqlMultiThreadExample._select_conn_count;
        min_conn = max_conn / 2;
        params = config()

        # creating separate connection for read and write purpose
        PsqlMultiThreadExample._insert_conn_pool = PsqlMultiThreadExample._select_conn_pool \
            = ThreadedConnectionPool(min_conn, max_conn, **params);

    def read_data(self):
        """
        This read thedata from the postgres and shared those records with each
        processor to perform their operation using threads
        Here we calculate the pardition value to help threading to read data from database

        :return:
        """
        pardition_value = 805000 / 10; # Its total record
        # this helps to identify the starting number to get data from db
        start_index = 1
        for pid in range(1, 11):
            # Getting connection from the connection pool
            select_conn = PsqlMultiThreadExample._select_conn_pool.getconn();
            insert_conn = PsqlMultiThreadExample._insert_conn_pool.getconn();
            #setting auto commit true
            insert_conn.autocommit = 1;
            # insert_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            #Creating 10 process to perform the operation
            ps = Process(target=self.process_data, args=(data_queque, pid, (start_index - 1) * pardition_value,
                                                               start_index * pardition_value, select_conn, insert_conn))
            start_index = start_index + 1;
            ps.daemon = True;
            ps.start();
            _start = time.time()
            ps.join()
            print ("Process %s took %s seconds" % (pid, (time.time() - _start)))

    def process_data(self, queue, pid, start_index, end_index, select_conn, insert_conn):
        """
        Here we process the each process into 10 multiple threads to do data process

        :param queue: 
        :param pid:
        :param start_index:
        :param end_index:
        :param select_conn:
        :param insert_conn:
        :return:
        """
        print ("\n");
        print(" \n Started processing record from %s to %s" % (start_index, end_index))
        pardition_value = (end_index - start_index) / 10;
        for tid in range(1, 11):
            ins_cur = insert_conn.cursor();
            worker = threading.Thread(target=self.process_thread, args=(
            queue, pid, tid, start_index, (start_index + pardition_value), select_conn.cursor(), ins_cur,
            threading.Lock()))
            start_index = start_index + pardition_value;
            worker.daemon = True;
            worker.start();
            worker.join()

    def process_thread(self, queue, pid, tid, start_index, end_index, sel_cur, ins_cur, lock):
        """
        Thread read data from database and doing the elatic search to get
        experience have the same data

        :param queue:
        :param pid:
        :param tid:
        :param start_index:
        :param end_index:
        :param sel_cur:
        :param ins_cur:
        :param lock:
        :return:
        """
        limit = end_index - start_index;
        sel_cur.execute(SELECT_QUERY,  (limit, start_index,))
        rows = sel_cur.fetchall();


        records.append(ins_cur.mogrify("(%s,%s,%s)", (row[0], row[1], row[2],)));



        self.write_data(records, ins_cur, lock)

    def write_data(self, records, ins_cur, lock):
        """
        Insert the data with experience id

        :param records:
        :param ins_cur:
        :param lock:
        :return:
        """

        lock.acquire()
        if records and records != '':
            ins_cur.execute(INSERT_QUERY + records)
        lock.release()


if __name__ == '__main__':
    _start = time.time()
    cmp_clener = PsqlMultiThreadExample();
    #Craeting database connection pool to help connection shared along process
    cmp_clener.create_connection_pool()
    cmp_clener.read_data();
    print('Total Processing time %s seconds' % (time.time() - _start))