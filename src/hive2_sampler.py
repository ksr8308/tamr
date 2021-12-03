#!/bin/python
import random
import time
import glob
import os
from pyhive import hive
from custom_logger import CustomLogger
from data_sampler import DataSampler


class Hive2Sampler(DataSampler):
    """
    This is a class to randomly sample data from the specified hive server and save
    sampled data into csv files, one file per table
    """
    # def __init__(self, host, port, authMechanism, user, password, maxNumPart=100):
    def __init__(self, config):

        # logger
        self.logger = CustomLogger("hive2_sampler")

        if 'host' not in config or 'port' not in config or 'authMechanism' not in config or 'user' not in config or \
                'pwd' not in config or 'maxNumPartition' not in config or 'getDataQuery' not in config:
            self.logger.error('Missing required configurations')
            exit(1)

        self._host = config['host']
        self._port = config['port']
        self._authMechanism = config['authMechanism']
        self._user = config['user']
        self._password = config['pwd']
        self._getDataQuery = config['getDataQuery']
        if config['maxNumPartition'] < 0:
            self.logger.error("Value for parameter maxNumPartition is illegal: {}".format(config['maxNumPartition']))
            self.logger.error("Will use the default value 100 for maxNumPartition instead")
            self._maxNumPart = 100
        elif config['maxNumPartition'] > 1000:
            self.logger.error("Value for parameter maxNumPartition is too large: {}".format(config['maxNumPartition']))
            self.logger.error("Will use the default value 100 for maxNumPartition instead")
            self._maxNumPart = 100
        else:
            self._maxNumPart = config['maxNumPartition']


    @staticmethod
    def _is_ascii(s):
        return all(ord(c) < 128 for c in s)

    def _get_connection(self, database):
        """
        Get connection for database
        :param database: database name
        :return: Connection if successful, otherwise None
        """
        self.logger.info("Getting connection for database '{}'".format(database))
        n_runs = 0
        conn = None
        while n_runs < 3:
            try:
                conn = hive.Connection(
                        host=self._host,
                        port=self._port,
                        username=self._user,
                        password=self._password,
                        database=database,
                        auth=self._authMechanism
                )
                break
            except Exception as e:
                self.logger.error(e)
                conn = None
                n_runs += 1
                time.sleep(1)
        if conn is None:
            self.logger.error("Failed to get connection to database '{}'".format(database))
        return conn

    def _get_databases(self, database):
        """
        Get all database names
        :return: List of database names if successful, otherwise empty list
        """
        try:
            conn = self._get_connection(database)
            if conn is not None:
                cur = conn.cursor()
                cur.execute("show databases")
                databases = cur.fetchall()
                databases = [y for x in databases for y in x]
                conn.close()
            else:
                databases = []
        except Exception as e:
            self.logger.error(e)
            databases = []
        return databases

    def get_tables(self, database):
        """
        Get all table names
        :param cur: Active cursor object
        :return: List of table names if successful, otherwise empty list
        """
        try:
            conn = self._get_connection(database)
            cur = conn.cursor()
            cur.execute("show tables")
            tables = cur.fetchall()
            tables = [y for x in tables for y in x]
            conn.close()
        except Exception as e:
            self.logger.error(e)
            tables = []
        return tables

    def _get_partitions(self, cur, table):
        """
        Get all partitions of table
        :param cur: Active cursor object
        :param table: Table name
        :return: List of partitions if successful, otherwise empty list
        """
        partitions = []
        n_runs = 0
        while n_runs < 3:
            try:
                cur.execute("show partitions " + table)
                result = cur.fetchall()
                partitions = [y for x in result for y in x]
                break
            except Exception as e:
                self.logger.info("Getting partitions failed on table '{}'".format(table))
                partitions = []
                n_runs += 1
                time.sleep(1)
        return partitions

    @staticmethod
    def _parse_partition(partition):
        """
        Parse partition string into logical condition clause
        :param partition: Partition string
        :return: Logical condition string
        """
        conditions = partition.split('/')
        clauses = []
        for condition in conditions:
            column, value = condition.split('=')
            clauses.append(column + '=' + '"' + value + '"')
        return ' AND '.join(clauses)

    def _get_data(self, database, table):
        """
        Get all data for database.table
        :param database: database name
        :param table: table name
        :return: A list of tuples, the first being the schema and others column values. None if Error
        """
        self.logger.info("Getting data for '{}.{}'".format(database, table))
        data = []
        conn = self._get_connection(database)
        if conn is None:
            return None
        cur = conn.cursor()
        partitions = self._get_partitions(cur, table)
        self.logger.info("Found partitions: {} ...".format(partitions[:10]))

        n_partitions = len(partitions)
        if n_partitions > 0:
            random.shuffle(partitions)
            if n_partitions > self._maxNumPart:
                partitions = partitions[:self._maxNumPart]
            query_size = int(10000. / len(partitions) + 1)
            schema = None
            i_part = 0
            for partition in partitions:
                self.logger.info('Processing partition {} of {}'.format(i_part+1, n_partitions))
                if i_part > 0 and i_part % 100 == 0:  # reset connection
                    conn.close()
                    conn = self._get_connection(database)
                    if conn is None:
                        self.logger.error("Failed to renew the connection to database '{}'".format(database))
                        return None
                    cur = conn.cursor()
                clause = self._parse_partition(partition)
                if i_part == 0:
                    self.logger.info("partition: {}".format(partition))
                    self.logger.info("clause:    {}".format(clause))
                # query = "select * from " + table
                query = self._getDataQuery.format(table)
                if clause != '':
                    query = query + ' where ' + clause
                query = query + ' limit {}'.format(query_size)
                # query = query + ' distribute by rand() sort by rand() limit 100'
                n_runs = 0
                while n_runs < 3:
                    try:
                        cur.execute(query)
                        if schema is None:
                            # result = cur.getSchema()
                            # schema = result
                            result = cur.description
                            result = [item[0] for item in result]
                            schema = tuple(result)
                            data.append(schema)
                        result = cur.fetchall()
                        if len(result) > 0:
                            data += result
                        break
                    except Exception as e:
                        self.logger.error(e)
                        time.sleep(5)
                        n_runs += 1
                if n_runs == 3:
                    self.logger.error("Failed to get data for partition '{}'".format(clause))
                i_part += 1
        else:  # non-partitioned table
            schema = None
            # query = 'select * from ' + table
            self.logger.info("{} is not partitioned.".format(table))
            query = self._getDataQuery.format(table)
            # query = query + ' limit 50000'
            query = query + ' distribute by rand() sort by rand() limit 10000'
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
                return None
        self.logger.info("Retrieved {} records from '{}.{}'".format(len(data), database, table))
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
        output_file = open(output_dir + '/' + database + '.' + table + '.csv', 'w')
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
                        [str(item).replace('"', '\\"').replace('\\\\"', '\\"') if item is not None
                         else '' for item in row]
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
        databases = self._get_databases()
        if len(databases) == 0:
            self.logger.error("No databases found")
            return False
        self.logger.info("Found databases {}".format(databases))
        for database in databases:
            conn = self._get_connection('test')
            if conn is not None:
                self.logger.info("Processing database '{}'".format(database))
                cur = conn.cursor()
                tables = self.get_tables(database)
                conn.close()
                self.logger.info("Found tables {} in database '{}'".format(tables, database))
                if len(tables) == 0:
                    continue
                for table in tables:
                    if not does_reload and self._get_file_size(database, table, output_dir) > 50000:
                        self.logger.info("Output file for '{}.{}' already exists. Skip.".format(database, table))
                        continue
                    data = self._get_data(database, table)
                    if data is None:
                        self.logger.error("Failed to get data for '{}.{}'".format(database, table))
                        continue
                    self._write_to_file(database, table, data, output_dir)
        return True

    def get_distinct_values(self, columns_file, output_dir):
        """
        Get distinct values in each column and save into file
        :param columns_file: File containing the full column path database.table.column
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
                database, table, column = line.split('.')
                if self._get_file_size(database, table + '.' + column, output_dir) > 50000:
                    continue
                self.logger.info("Getting distinct values for '{}.{}.{}'".format(database, table, column))
                conn = self._get_connection(database)
                if conn is not None:
                    cur = conn.cursor()
                    partitions = self._get_partitions(cur, table)
                    n_partitions = len(partitions)
                    if n_partitions > 0:
                        i_part = 0
                        for partition in partitions:
                            if i_part > 0 and i_part % 100 == 0:
                                conn.close()
                                conn = self._get_connection(database)
                                cur = conn.cursor()
                            self.logger.info('Processing partition {} of {}'.format(i_part+1, n_partitions))
                            clause = self._parse_partition(partition)
                            if i_part == 0:
                                self.logger.info('partition: {}'.format(partition))
                                self.logger.info('clause:    {}'.format(clause))
                            query = 'select distinct(' + column + ') from ' + table
                            if clause != '':
                                query = query + ' where ' + clause
                            n_runs = 0
                            while n_runs < 3:
                                try:
                                    cur.execute(query)
                                    if schema is None:
                                        schema = ('primaryKey', column)
                                        data.append(schema)
                                        # self.logger.info(schema)
                                    result = cur.fetchall()
                                    if len(result) > 0:
                                        distinct_values = distinct_values.union([y for x in result for y in x])
                                    break
                                except Exception as e:
                                    self.logger.error(e)
                                    time.sleep(5)
                                    n_runs += 1
                            i_part += 1
                    else:
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
                    self._write_to_file(database, table + '.' + column, data, output_dir)

