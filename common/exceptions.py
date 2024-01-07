import logging

logger = logging.getLogger(__name__)


class BaseException(Exception):
    """Global exception class for Invera."""

    def __init__(self, message=None):
        """Do something useful with extra params."""
        # Call the base class constructor with the parameters it needs
        logger.error("Exception raised: %s", message)
        super().__init__(message)
        self.message = message


class ParameterError(BaseException):
    pass
