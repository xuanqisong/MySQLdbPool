import MySQLdb
import logging
import Queue
from threading import Thread
from contextlib import contextmanager
import time


class MysqlTool(object):
    """
    .. version 1.0
    :param is a dict has db connect parameter

    """

    def __new__(cls, parameter_dict, *args, **kwargs):
        dict_keys = ["host", "port", "user", "password", "db"]
        if isinstance(parameter_dict, dict):
            for keys in dict_keys:
                if keys not in parameter_dict:
                    raise KeyError("parameter do not have" + keys)
                return super(MysqlTool, cls).__new__(cls)
        raise TypeError("parameter is not a dict")

    def __init__(self, parameter_dict):
        self.host = parameter_dict['host']
        self.port = parameter_dict['port']
        self.user = parameter_dict['user']
        self.pwd = parameter_dict['password']
        self.db = parameter_dict['db']
        self.charset = parameter_dict.get('charset', 'utf8mb4')
        self.logger = logging.getLogger("baseClass.mysql")
        self.conn = None
        self.cu = None

    def getConnect(self, autocommit=True):
        try:
            self.conn = MySQLdb.Connect(host=self.host, port=self.port, user=self.user,
                                        passwd=self.pwd, db=self.db, charset=self.charset)
            self.conn.autocommit(autocommit)
        except Exception as e:
            self.conn = None
            self.logger.exception("GET MYSQL CONNECT ERROR")
            raise e

    def disConnect(self):
        if self.cu:
            self.cu.close()
        if self.conn:
            self.conn.close()

    def runSelect(self, sql, value_list=None, format_result=False):
        if self.conn:
            if format_result:
                self.cu = self.conn.cursor(MySQLdb.cursors.DictCursor)
            else:
                self.cu = self.conn.cursor()

            try:
                if value_list is None:
                    self.cu.execute(sql)
                else:
                    self.cu.execute(sql, value_list)

                return self.cu.fetchall()

            except Exception as e:
                self.logger.exception("SELECT DATA ERROR")
                raise e
        else:
            self.logger.error("No Mysql Connect")
            raise Exception("No Mysql Connect")

    def runInsertDeleteUpdate(self, sql, value_list=None, primary_key=None):
        if self.conn:
            self.cu = self.conn.cursor()
            try:
                if value_list is None:
                    execute_num = self.cu.execute(sql)
                else:
                    execute_num = self.cu.execute(sql, value_list)

                if primary_key:
                    return execute_num, self.cu.lastrowid
                else:
                    return execute_num

            except Exception as e:
                self.logger.exception("UPDATE OR DELETE ERROR")
                raise e
        else:
            self.logger.error("No Mysql Connect")
            raise Exception("No Mysql Connect")

    def __enter__(self):
        self.getConnect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disConnect()
        if exc_type:
            logging.error("CLOSE CONNECT ERROR", exc_val)
        return True


class ConnectionsPool(Thread):
    """
    .. version 1.0
    :param parameter_dict is a connections parameter

    :param connect_class is a connections class

    :param max_size pool has max connections

    :param min_size pool has min connections
    """
    _class_queue = None
    _has_size = 0

    def __init__(self, parameter_dict, connect_class=MysqlTool, max_size=5, min_size=3):
        # TODO now only can use in MysqlTool
        super(ConnectionsPool, self).__init__()
        self.connect_class = connect_class
        self.parameter_dict = parameter_dict

        self._max_size = max_size
        self._min_size = min_size

        self._run_check = True
        self._error_check = True

    def __make_connections(self):
        self.__class__._class_queue = Queue.Queue(self._min_size + 1)
        for i in range(0, self._min_size):
            try:
                _temp_mysql_class = self.connect_class(self.parameter_dict)
            except TypeError as e:
                self._error_check = False
                raise e
            except AttributeError as e:
                self._error_check = False
                raise e
            _temp_mysql_class.getConnect()
            self.__class__._class_queue.put(_temp_mysql_class)
            self.__class__._has_size += 1

    def run(self):
        self.__make_connections()
        self.check_error()

        while self._run_check and self._error_check:
            time.sleep(1)
            _check_mysql_class = self.__class__._class_queue.get()
            if _check_mysql_class.conn.open:
                self.__class__._class_queue.put(_check_mysql_class)
                continue
            else:
                _queue_size = self.__class__._class_queue.qsize()
                if _queue_size > self._min_size:
                    self.__class__._has_size -= 1
                    continue
                _check_mysql_class = self.connect_class.getConnect(self.parameter_dict)
                self.__class__._class_queue.put(_check_mysql_class)

        while True and self._error_check:
            if self.__class__._has_size == 0:
                break
            if self.__class__._class_queue.qsize() > 0:
                _connect_class = self.__class__._class_queue.get()
                _connect_class.disConnect()
                self.__class__._has_size -= 1

    @contextmanager
    def connection_pool(self, timeout=3):
        self.check_error()
        try:
            connection = self.__class__._class_queue.get(timeout=timeout)
        except Queue.Empty:
            if self.__class__._has_size < self._max_size:
                connection = self.connect_class.getConnect(self.parameter_dict)
                self.__class__._has_size += 1
            else:
                try:
                    connection = self.__class__._class_queue.get(timeout=60)
                except Queue.Empty:
                    raise Exception("Beyond Max Connections")

        yield connection
        self.__class__._class_queue.put(connection)

    def set_runCheck(self, run_check):
        self._run_check = run_check

    def check_error(self):
        if False is self._error_check:
            raise Exception("Please Check logs, Connections has error")
