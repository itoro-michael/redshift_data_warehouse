import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Extracts data from log files and loads to staging tables.
    Args:
        cur (object): The database cursor object.
        conn (object): The database connection object.
    Returns:
        None:
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Extracts data from staging tables and loads to analytical tables.
    Args:
        cur (object): The database cursor object.
        conn (object): The database connection object.
    Returns:
        None:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Module main function
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()