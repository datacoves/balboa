import logging

from datacoves_connection import get_connection_properties

import snowflake.connector
from snowflake.connector import ProgrammingError
from snowflake.connector import DictCursor
import threading


import time

snowflake_auth_info = get_connection_properties()#target='prd')

snowflake_connection = snowflake.connector.connect(
    account = snowflake_auth_info['account'],
    warehouse = snowflake_auth_info['warehouse'],
    database = snowflake_auth_info['database'],
    # role = snowflake_auth_info['role'],
    role = "TRANSFORMER_DBT",
    schema = snowflake_auth_info['schema'],
    user = snowflake_auth_info['user'],
    private_key_path = snowflake_auth_info['private_key_path'],
    private_key = snowflake_auth_info['private_key'],
    # password = snowflake_auth_info['account'],
    session_parameters={
        'QUERY_TAG': 'cloned_db_tables',
    }

)



def main():

    source_database = "BALBOA"
    target_database = "BALBOA_STAGING_TEST"

    cursor = snowflake_connection.cursor()
    dict_cursor = snowflake_connection.cursor(DictCursor)

    # Create target database
    query = f'create database if not exists {target_database}'
    db_created = run_snowflake_command(cursor, query)
    print(db_created)

    # Get schemas to clone
    query = f"show schemas in database {source_database};"
    schemas = run_snowflake_command(dict_cursor, query)
    names = [schema['name'] for schema in schemas ]

    # schemas = run_snowflake_command(cursor, query)

    # print(schemas)
    # print("=====")
    # print(schemas[0]['SCHEMA_NAME'])
        # schemas = dict_cursor.fetchall()

    # schemas = dict_cursor.fetchall()

    print(names)
    clone_database_schemas(source_database, target_database)


def clone_database_schemas(source_database, target_database):
    dict_cursor = snowflake_connection.cursor(DictCursor)
    dict_cursor.execute(f"show schemas in database {source_database};")
    schemas = dict_cursor.fetchall()
    threaded_run_commands = ThreadedRunCommands(snowflake_connection, 10)
    for schema in schemas:
        # if schema['name'] not in self._list_of_schemas_to_exclude:
            # Clone each schema
        threaded_run_commands.register_command(
            f"create schema {target_database}.{schema['name']} clone {source_database}.{schema['name']};")
    threaded_run_commands.run()


class ThreadedRunCommands:
    """Helper class for running queries across a configurable number of threads"""

    def __init__(self, con, threads):
        self.threads = threads
        self.register_command_thread = 0
        self.thread_commands = [
            [] for _ in range(self.threads)
        ]
        self.con = con

    def register_command(self, command):
        self.thread_commands[self.register_command_thread].append(command)
        if self.register_command_thread + 1 == self.threads:
            self.register_command_thread = 0
        else:
            self.register_command_thread += 1

    def run_command(self, command):
        print(command)
        self.con.cursor().execute_async(command)

    def run_commands(self, commands):
        for command in commands:
            self.run_command(command)

    def run(self):
        procs = []
        for v in self.thread_commands:
            proc = threading.Thread(target=self.run_commands, args=(v,))
            procs.append(proc)
            proc.start()
        # complete the processes
        for proc in procs:
            proc.join()



def run_snowflake_command(cursor,query):
    try:
        cursor.execute(query)
        results = cursor.fetchall()
    except snowflake.connector.errors.ProgrammingError as e:
        # default error message
        print(e)
        # customer error message
        # print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
    finally:
        cursor.close()

    return results


if __name__ == "__main__":
    try:
        main()

    except Exception as ex:
        logging.info(ex)
        exit(1)
