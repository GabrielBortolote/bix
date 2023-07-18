# built-in imports
import logging

# third-part imports
from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import OperationalError, ProgrammingError, DatabaseError

# get pre-defined logger
logger = logging.getLogger('default_logger')

def postgres_connection(username:str, password:str, database_name:str,
                        host:str = 'localhost', port: str = '5432') -> Engine:
    """
    This function creates a connection to a PostgreSQL database.

    Parameters:
        - username (str): The username used to connect to the database.
        - password (str): The password associated with the username for authentication.
        - database_name (str): The name of the PostgreSQL database to connect to.
        - host (str, optional): The host address where the database is running. Default is
        'localhost'.
        - port (str, optional): The port number for the database connection. Default is '5432',
        the default port for PostgreSQL.

    Returns:
        - connection (sqlalchemy.Engine): the engine able to do the connection with the database.
    """
    try:
        # Construct the connection string
        connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'
        
        # Create the SQLAlchemy engine
        return create_engine(connection_string)

    except OperationalError as e:
        logger.error('Issues with the connection to the database server, like incorrect credentials or server unavailability.')
        raise e

    except ProgrammingError as e:
        logger.error('Issues with the SQL statement, like \syntax error or referencing non-existing tables/columns.')
        raise e

    except DatabaseError as e:
        logger.error('This is a general exception for database-related errors that don\'t fit into the specific categories above.')
        raise e