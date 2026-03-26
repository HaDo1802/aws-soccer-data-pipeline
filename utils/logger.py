import logging
import os


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Always log to console (CloudWatch in Lambda)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Optional: local file logging only
    if not os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        try:
            file_handler = logging.FileHandler("logs/scraper.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except OSError:
            pass

    return logger