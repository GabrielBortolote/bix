import logging

def create_logger(path):

    # Create a logger
    logger = logging.getLogger('default_logger')
    logger.setLevel(logging.DEBUG)

    # Create a console handler and set the log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Set the desired console log level

    # Create a file handler and set the log level
    file_handler = logging.FileHandler(path)
    file_handler.setLevel(logging.DEBUG)  # Set the desired file log level

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s[%(levelname)s] %(message)s')

    # Add the formatter to both handlers
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger