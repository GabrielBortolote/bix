"""
This module implements the function create_logger
"""

import os
import logging

def create_logger(path:str) -> logging.Logger:
    """
    This function configures an customized log object, the default name to the logger is
    'default_logger'. This logger are going to display images on the terminal and save
    the same messages on a file, the name of the file is going to be the current timestamp
    and the location is being pointed by the path parameter.

    Parameters:
        - path (str): the path to the directory where to save the log

    Returns:
        - logger (logging.Logger): a logger object
    """
    # create a file handler and set the log level
    if not os.path.isdir(os.path.dirname(path)):
        raise Exception('The provided path is not a valid path')

    # create a logger
    logger = logging.getLogger('default_logger')
    logger.setLevel(logging.DEBUG)

    # create a console handler and set the log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # create a file handler and set the log level
    file_handler = logging.FileHandler(path, mode='w')
    file_handler.setLevel(logging.DEBUG)

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s[%(levelname)s] %(message)s')

    # Add the formatter to both handlers
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger