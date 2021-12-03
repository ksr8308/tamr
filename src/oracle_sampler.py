#!/bin/python
import time
import glob
import os
import cx_Oracle
from custom_logger import CustomLogger
from data_sampler import DataSampler


class OracleSampler(DataSampler):
    """
    This is a class to randomly sample data from the specified Oracle server and save
    sampled data into csv files, one file per table
    """
    def __init__(self, config):

        # logger
        self.logger = CustomLogger("oracle_sampler")

        if 'host' not in config or 'port' not in config or 'user' not in config or 'pwd' not in config \
                or 'sid' not in config or 'getTableQuery' not in config or 'getDataQuery' not in config:
            self.logger.error('Missing required configurations')
            exit(1)

        self._host = config['host']
        self._port = config['port']
        self._user = config['user']
        self._password = config['pwd']
        self._sid = config['sid']
        self._getTableQuery = config['getTableQuery']
        self._getDataQuery = config['getDataQuery']

    @staticmethod
    def _is_ascii(s):
        return all(ord(c) < 128 for c in s)

    def _get_connection(self):
        """
        Get connection for database
        :return: Connection if successful, otherwise None
        """
        self.logger.info("Getting connection for sid '{}'".format(self._sid))
        n_runs = 0
        conn = None
        while n_runs < 3:
            try:				
                conn = cx_Oracle.Connection(
                    self._user,
                    self._password,
                    "{}:{}/{}".format(self._host, self._port, self._sid)
                )
                break
            except Exception as e:
                self.logger.error(e)
                conn = None
                n_runs += 1
                time.sleep(1)
        if conn is None:
            self.logger.error("Failed to get connection to sid '{}'".format(self._sid))
        return conn

    def get_tables(self):
        """
        Get all table names
        :param cur: Active cursor object
        :return: List of table names if successful, otherwise empty list
        """
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            self.logger.debug(self._getTableQuery)
            cur.execute(self._getTableQuery)
            tables = cur.fetchall()
            tables = [table[0] for table in tables]
        except Exception as e:
            self.logger.error(e)
            conn.close()
            tables = []
        conn.close()
        return tables

    def _get_data(self, table, conn):
        """
        Get all data for database.table
        :param table: table name
        :return: A list of tuples, the first being the schema and others column values. None if Error
        """
        self.logger.info("Getting data for '{}.{}'".format(self._sid, table))
        data = []
        # conn = self._get_connection()
        # if conn is None:
        #     return None
        cur = conn.cursor()

        schema = None
        # query = 'select * from (select * from ' + table + ' order by dbms_random.value) where rownum < 10000'
        query = self._getDataQuery.format(table)
        n_runs = 0
        while n_runs < 3:
            try:
                cur.execute(query)
                if schema is None:
                    result = cur.description
                    result = [item[0] for item in result]
                    schema = tuple(result)
                    data.append(schema)
                result = cur.fetchall()
                if len(result) > 0:
                    data += result
                break
            except Exception:
                time.sleep(5)
                n_runs += 1
        if n_runs == 3:
            conn.close()
            return None
        self.logger.info("Retrieved {} records from '{}.{}'".format(len(data), self._sid, table))
        # conn.close()
        return data

    def _write_to_file(self, database, table, data, output_dir):
        """
        Write sampled data into csv file
        :param database: Database name
        :param table: Table name
        :param data: Data as a list of tuples
        :param output_dir: Absolute path to output directory
        :return: None
        """
        self.logger.info("Saving data to file for '{}.{}'".format(database, table))
        output_file = open(output_dir + '/' + database + '.' + table + '.csv', 'w', encoding='utf-8')
        if len(data) > 0:
            schema = data[0]
            column_names = []
            for columnName in schema:
                if '.' in columnName:
                    column_names.append(columnName.split('.')[1])
                else:
                    column_names.append(columnName)
            output_file.write('' + ','.join(column_names) + '\n')
            for row in data[1:]:
                output_file.write(
                    '"' +
                    '","'.join(
                        [str(item).replace('"', '\\"').replace('\\\\"', '\\"') if item is not None else '' for item in row]
                    ) +
                    '"\n'
                )
        output_file.close()

    @staticmethod
    def _get_file_size(database, table, output_dir):
        """
        Get existing local output file size
        :param database: Database
        :param table: Table name
        :param output_dir: Absolute path to output directory
        :return: File size
        """
        filesize = 0
        filepath = output_dir + '/' + database + '.' + table + '.csv'
        files = glob.glob(filepath)
        if len(files) == 1:
            filesize = int(os.path.getsize(filepath))
        return filesize

    def get_sampled_data(self, output_dir, does_reload=False):
        """
        Sample data from all databases/tables in hive server and save into files
        :param output_dir: Absolute path to output directory
        :return: True if successful. Otherwise False
        """
        self.logger.info("Retrieving random sample data from all databases/tables")
        self.logger.info("Data will be saved under {}".format(output_dir))
        conn = self._get_connection()
        if conn is not None:
            conn.close()
            self.logger.info("Processing sid '{}'".format(self._sid))
            tables = self.get_tables()
            self.logger.info("Found tables {} in sid '{}'".format(tables, self._sid))
            if len(tables) == 0:
                self.logger.error("No tables found")
                return False
            for table in tables:
                if not does_reload and self._get_file_size(self._sid, table, output_dir) > 50000:
                    self.logger.info("Output file for '{}.{}' already exists. Skip.".format(self._sid, table))
                    continue
                conn = self._get_connection()
                if conn is None:
                    return None
                data = self._get_data(table, conn)
                if data is None:
                    self.logger.error("Failed to get data for '{}.{}'".format(self._sid, table))
                    continue
                self._write_to_file(self._sid, table, data, output_dir)
                conn.close()
        return True

    def get_distinct_values(self, columns_file, output_dir):
        """
        Get distinct values in each column and save into file
        :param columns_file: File containing the full column path sid.table.column
        :param output_dir: Absolute path to output directory
        :return: None
        """
        self.logger.info("Processing columns in {}".format(columns_file))
        with open(columns_file, 'r') as columns:
            for line in columns.readlines():
                if '.' not in line:
                    continue
                distinct_values = set()
                data = []
                schema = None
                line = line[:-1]
                sid, table, column = line.split('.')
                if self._get_file_size(sid, table + '.' + column, output_dir) > 50000:
                    continue
                self.logger.info("Getting distinct values for '{}.{}.{}'".format(sid, table, column))
                conn = self._get_connection()
                if conn is not None:
                    cur = conn.cursor()
                    query = 'select distinct(' + column + ') from ' + table
                    n_runs = 0
                    while n_runs < 3:
                        try:
                            cur.execute(query)
                            if schema is None:
                                schema = ('primaryKey', column)
                                data.append(schema)
                            result = cur.fetchall()
                            if len(result) > 0:
                                distinct_values = distinct_values.union([y for x in result for y in x])
                            break
                        except Exception as e:
                            self.logger.error(e)
                            time.sleep(5)
                            n_runs += 1
                    conn.close() 
                if len(distinct_values) > 0:
                    list_distinct_values = []
                    for value in distinct_values:
                        if value is None or value == '' or value.strip() == '':
                            continue
                        elif self._is_ascii(value):
                            list_distinct_values.append(value)
                    for i in range(0, len(list_distinct_values)):
                        data.append([str(i), list_distinct_values[i]])
                    # data += [[item] for item in list_distinct_values]
                    self._write_to_file(sid, table + '.' + column, data, output_dir)

