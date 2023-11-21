# Import necessary libraries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Function to drop tables
def drop_tables(cur, conn):
    # Iterate through drop table queries and execute them
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Function to create tables
def create_tables(cur, conn):
    # Iterate through create table queries and execute them
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Main function
def main():
    # Read configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        # Establish a connection to the database
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        # Drop existing tables
        drop_tables(cur, conn)

        # Create new tables
        create_tables(cur, conn)

    except Exception as e:
        # Print error message if an exception occurs
        print("Error:", e)

    finally:
        # Close the connection in the finally block to ensure it happens regardless of success or failure
        if conn is not None:
            conn.close()

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()
