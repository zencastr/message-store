"""Try and use zenai logger, otherwise use logging."""

try:
    from zenai.logger import logger
    message_store_logger = logger
except ImportError:
    import logging
    message_store_logger = logging.getLogger("MessageStore")
