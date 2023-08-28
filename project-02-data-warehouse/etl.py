import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load data into Redshift staging tables including events and songs from S3
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Insert data into dim, fact tables from staging tables in Redshift
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Load data from S3 into staging tables and insert data to dim, fact tables in the Redshift
    '''

    # Get the params of the created redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data into Redshift staging tables including events and songs from S3
    load_staging_tables(cur, conn)
    # Insert data into dim, fact tables from staing tables in Redshift
    insert_tables(cur, conn)

    # Close connection to the Redshift Cluster
    conn.close()


if __name__ == "__main__":
    main()