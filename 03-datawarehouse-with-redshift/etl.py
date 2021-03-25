import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 to Redshift cluster"""
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transform data from staging tables to start schema tables"""
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """ETL from S3 to Redshift cluster"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    connection_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    print("connection_string:", connection_string)
    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()