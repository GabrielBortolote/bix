# built-in imports
import logging

# third-part imports
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError, DatabaseError

# get pre-defined logger
logger = logging.getLogger('default_logger')

def postgres_connection(
    username,
    password,
    database_name,
    host = 'localhost',
    port = '5432',
):
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