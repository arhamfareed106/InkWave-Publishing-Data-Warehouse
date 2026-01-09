"""
InkWave Publishing Data Warehouse - Oracle Database Connector
Author: Data Engineering Team
Date: 2025-12-07
Purpose: Database connection pool management with retry logic
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import time

# Note: In production, use: import oracledb as cx_Oracle
# For this demo, we'll create a mock interface
try:
    import cx_Oracle
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False
    logging.warning("cx_Oracle not available - using mock interface")

from config.config import ORACLE_CONFIG, get_oracle_dsn, ETL_CONFIG

logger = logging.getLogger('inkwave.oracle_connector')


class OracleConnectionPool:
    """
    Oracle database connection pool with automatic retry and error handling.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize connection pool.
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config or ORACLE_CONFIG
        self.pool = None
        self.retry_attempts = ETL_CONFIG['max_retries']
        self.retry_delay = ETL_CONFIG['retry_delay_seconds']
        
    def create_pool(self) -> bool:
        """
        Create connection pool.
        
        Returns:
            bool: True if pool created successfully
        """
        if not ORACLE_AVAILABLE:
            logger.warning("Oracle client not available - running in demo mode")
            return True
            
        try:
            dsn = get_oracle_dsn()
            
            logger.info(f"Creating connection pool to {dsn}")
            
            self.pool = cx_Oracle.SessionPool(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn,
                min=self.config['pool_min'],
                max=self.config['pool_max'],
                increment=self.config['pool_increment'],
                encoding=self.config['encoding'],
                threaded=True
            )
            
            logger.info(f"Connection pool created successfully (min={self.config['pool_min']}, max={self.config['pool_max']})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            return False
    
    def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            try:
                self.pool.close()
                logger.info("Connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {str(e)}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager to get a connection from the pool.
        
        Yields:
            Connection object
        """
        if not ORACLE_AVAILABLE:
            # Return mock connection for demo
            yield MockConnection()
            return
            
        connection = None
        try:
            connection = self.pool.acquire()
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Connection error: {str(e)}")
            raise
        finally:
            if connection:
                self.pool.release(connection)
    
    def execute_query(self, query: str, params: Optional[Dict] = None, 
                     fetch_all: bool = True) -> List[Tuple]:
        """
        Execute a SELECT query with automatic retry.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch_all: Whether to fetch all results
            
        Returns:
            List of result tuples
        """
        for attempt in range(self.retry_attempts):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params or {})
                    
                    if fetch_all:
                        results = cursor.fetchall()
                    else:
                        results = cursor.fetchone()
                    
                    cursor.close()
                    return results
                    
            except Exception as e:
                logger.warning(f"Query attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Query failed after {self.retry_attempts} attempts")
                    raise
    
    def execute_dml(self, query: str, params: Optional[Dict] = None) -> int:
        """
        Execute INSERT, UPDATE, DELETE with automatic retry.
        
        Args:
            query: SQL DML statement
            params: Statement parameters
            
        Returns:
            Number of rows affected
        """
        for attempt in range(self.retry_attempts):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params or {})
                    rows_affected = cursor.rowcount
                    cursor.close()
                    return rows_affected
                    
            except Exception as e:
                logger.warning(f"DML attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"DML failed after {self.retry_attempts} attempts")
                    raise
    
    def execute_batch(self, query: str, data: List[Tuple], 
                     commit_interval: int = None) -> int:
        """
        Execute batch INSERT with periodic commits.
        
        Args:
            query: INSERT statement
            data: List of tuples with row data
            commit_interval: Commit after N rows
            
        Returns:
            Total rows inserted
        """
        if not commit_interval:
            commit_interval = ETL_CONFIG['commit_interval']
        
        total_inserted = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                for i, row in enumerate(data, 1):
                    cursor.execute(query, row)
                    total_inserted += 1
                    
                    if i % commit_interval == 0:
                        conn.commit()
                        logger.debug(f"Batch committed: {i} rows")
                
                # Final commit
                conn.commit()
                logger.info(f"Batch insert complete: {total_inserted} rows")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Batch insert failed at row {total_inserted + 1}: {str(e)}")
                raise
            finally:
                cursor.close()
        
        return total_inserted
    
    def call_procedure(self, proc_name: str, params: List[Any]) -> Any:
        """
        Call stored procedure.
        
        Args:
            proc_name: Procedure name
            params: List of parameters
            
        Returns:
            Procedure result
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            result = cursor.callproc(proc_name, params)
            cursor.close()
            return result
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query, fetch_all=False)
        return result[0] if result else 0
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.
        
        Args:
            table_name: Table name
            
        Returns:
            True if table exists
        """
        query = """
            SELECT COUNT(*) 
            FROM user_tables 
            WHERE table_name = UPPER(:table_name)
        """
        result = self.execute_query(query, {'table_name': table_name}, fetch_all=False)
        return result[0] > 0 if result else False


class MockConnection:
    """Mock connection for demo purposes when Oracle client is not available."""
    
    def commit(self):
        pass
    
    def rollback(self):
        pass
    
    def cursor(self):
        return MockCursor()


class MockCursor:
    """Mock cursor for demo purposes."""
    
    def __init__(self):
        self.rowcount = 0
    
    def execute(self, query: str, params: Optional[Dict] = None):
        pass
    
    def fetchall(self):
        return []
    
    def fetchone(self):
        return None
    
    def close(self):
        pass
    
    def callproc(self, proc_name: str, params: List[Any]):
        return params


# Global connection pool instance
_connection_pool: Optional[OracleConnectionPool] = None


def get_connection_pool() -> OracleConnectionPool:
    """
    Get global connection pool instance (singleton pattern).
    
    Returns:
        OracleConnectionPool instance
    """
    global _connection_pool
    
    if _connection_pool is None:
        _connection_pool = OracleConnectionPool()
        _connection_pool.create_pool()
    
    return _connection_pool


def close_connection_pool():
    """Close global connection pool."""
    global _connection_pool
    
    if _connection_pool:
        _connection_pool.close_pool()
        _connection_pool = None


if __name__ == "__main__":
    # Test connection pool
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Oracle Connection Pool")
    print("=" * 60)
    
    pool = OracleConnectionPool()
    
    if pool.create_pool():
        print("✓ Connection pool created")
        
        # Test query
        try:
            result = pool.execute_query("SELECT SYSDATE FROM DUAL", fetch_all=False)
            print(f"✓ Test query executed: {result}")
        except Exception as e:
            print(f"✗ Test query failed: {str(e)}")
        
        pool.close_pool()
        print("✓ Connection pool closed")
    else:
        print("✗ Failed to create connection pool")
