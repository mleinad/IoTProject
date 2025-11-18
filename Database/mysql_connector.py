import mysql.connector
from mysql.connector import Error, pooling

def connect_mysql(MYSQLCONNECTOR, database):
    conn = None
    try:
        if database:
            conn = mysql.connector.connect(
                host=MYSQLCONNECTOR[0],
                user=MYSQLCONNECTOR[1],
                password=MYSQLCONNECTOR[2],
                database=database,
                charset='utf8mb4',
                use_unicode=True
            )
        else:
            conn = mysql.connector.connect(
                host=MYSQLCONNECTOR[0],
                user=MYSQLCONNECTOR[1],
                password=MYSQLCONNECTOR[2],
                charset='utf8mb4',
                use_unicode=True
            )
        
        if conn.is_connected():
            db_info = conn.get_server_info()
            if database:
                print(f"Connected to MySQL Server version")
                print(f"Connected to database: '{database}'")
            else:
                print(f"Connected to MySQL Server version")
                
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        
    return conn

# nao sei se vamos usar pooling mas fica aqui
def create_connection_pool(MYSQLCONNECTOR, database, pool_name="mypool", pool_size=5):
    """
    Create a MySQL connection pool for better performance with multiple connections.
    
    Args:
        MYSQLCONNECTOR: List containing [host, user, password]
        database: Database name
        pool_name: Name for the connection pool (default: "mypool")
        pool_size: Number of connections in the pool (default: 5, max: 32)
    
    Returns:
        MySQLConnectionPool object or None if creation fails
    """
    try:
        dbconfig = {
            "host": MYSQLCONNECTOR[0],
            "user": MYSQLCONNECTOR[1],
            "password": MYSQLCONNECTOR[2],
            "database": database,
            "charset": "utf8mb4",
            "use_unicode": True
        }
        
        connection_pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            pool_reset_session=True,
            **dbconfig
        )
        
        print(f"Connection pool '{pool_name}' created with {pool_size} connections")
        return connection_pool
        
    except Error as e:
        print(f"Error creating connection pool: {e}")
        return None


def get_pooled_connection(connection_pool):
    """
    Get a connection from the connection pool.
    
    Args:
        connection_pool: MySQLConnectionPool object
    
    Returns:
        Connection object or None if retrieval fails
    """
    try:
        connection = connection_pool.get_connection()
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error getting connection from pool: {e}")
        return None
