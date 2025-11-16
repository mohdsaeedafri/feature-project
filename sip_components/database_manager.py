"""
DatabaseManager - Centralized MySQL operations with connection pooling and retry logic
"""
import mysql.connector
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from mysql.connector import pooling

class DatabaseManager:
    def __init__(self):
        """Initialize database manager with connection pool"""
        self.pool = self._create_connection_pool()
        
    def _create_connection_pool(self):
        """Create a connection pool for database connections"""
        try:
            pool_config = {
                'pool_name': 'sip_pool',
                'pool_size': 5,
                'pool_reset_session': True,
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'host': os.getenv('DB_HOST'),
                'database': os.getenv('DB_NAME')
            }
            
            # Add SSL configuration if enabled
            if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and os.getenv('SSL_CA'):
                pool_config['ssl_ca'] = os.getenv('SSL_CA')
                logging.info("SSL connection enabled")
                
            return pooling.MySQLConnectionPool(**pool_config)
        except Exception as e:
            logging.error(f"Failed to create connection pool: {e}")
            raise
            
    def get_connection(self):
        """Get a connection from the pool"""
        try:
            return self.pool.get_connection()
        except Exception as e:
            logging.error(f"Failed to get connection from pool: {e}")
            raise
            
    def execute_query(self, query):
        """Execute a single query and return results as DataFrame"""
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, con=conn)
            conn.close()
            return df
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise
            
    def execute_parallel_queries(self, queries):
        """Execute multiple queries in parallel and return results"""
        def fetch_query(query):
            local_conn = self.get_connection()
            df = pd.read_sql(query, local_conn)
            local_conn.close()
            return df
            
        try:
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(fetch_query, query) for query in queries]
                results = [future.result() for future in futures]
            return results
        except Exception as e:
            logging.error(f"Failed to execute parallel queries: {e}")
            raise
            
    def get_max_period(self, table_name):
        """Get the maximum period from a table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX(Period) FROM {table_name}")
            max_period = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return max_period
        except Exception as e:
            logging.error(f"Failed to get max period from {table_name}: {e}")
            raise
    
    def fetch_population_data(self):
        """Fetch population data from population_data_by_age_and_sex table"""
        try:
            pop_sql = """
            SELECT
              TRIM(usps_state_name) AS usps_state_name,
              CAST(zip_code AS CHAR(5)) AS zip_code,
              estimate_total_population
            FROM population_data_by_age_and_sex
            """
            return self.execute_query(pop_sql)
        except Exception as e:
            logging.error(f"Failed to fetch population data: {e}")
            raise