import logging
import os

try:
    from pythonjsonlogger import jsonlogger  # pylint: disable=import-error
    HAS_JSON_LOGGER = True
except ImportError:
    HAS_JSON_LOGGER = False


def configure_logging():
    logger = logging.getLogger('fudly')
    if logger.handlers:
        return logger

    level_name = os.environ.get('LOG_LEVEL', 'INFO')
    level = getattr(logging, level_name.upper(), logging.INFO)

    handler = logging.StreamHandler()
    
    if HAS_JSON_LOGGER:
        fmt = jsonlogger.JsonFormatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    else:
        # Fallback to standard logging format
        fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler.setFormatter(fmt)

    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = configure_logging()
