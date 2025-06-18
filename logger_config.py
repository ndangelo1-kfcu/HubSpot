import os
import logging
from logging.handlers import TimedRotatingFileHandler

# Ensure the logs directory exists
log_directory = os.path.join(os.path.dirname(__file__), "logs")


def setup_logger():
    os.makedirs(log_directory, exist_ok=True)

    # Configure logging to log to both a file and the console
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all log messages

    # Create a timed rotating file handler
    file_handler = TimedRotatingFileHandler(
        os.path.join(log_directory, "process_log.log"),
        when="midnight",
        interval=1,
        backupCount=120,  # Keep logs for 120 days
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Enable SQLAlchemy engine logging
    sqlalchemy_logger = logging.getLogger("sqlalchemy.engine")
    sqlalchemy_logger.setLevel(logging.INFO)  # Set to DEBUG for detailed SQL logs
    sqlalchemy_logger.addHandler(file_handler)
    sqlalchemy_logger.addHandler(console_handler)

    return logger


# Initialize the logger
logger = setup_logger()
