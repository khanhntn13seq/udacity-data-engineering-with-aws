import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    '''
    Drop all of tables in data warehouse including staging, dim, fact tables.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create all of tables including staging, dim, fact tables.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Drop tables if exist and create the new one in Sparkify data warehouse. 
    '''

    # Get the params of the created redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to the Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop tables if exist
    drop_tables(cur, conn)
    # Create tables
    create_tables(cur, conn)

    # Close connection to the Redshift Cluster
    conn.close()


if __name__ == "__main__":
    main()