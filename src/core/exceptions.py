class CDCException(Exception):
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}

class ConsumerException(CDCException):
    pass

class WriterException(CDCException):
    pass

class SchemaException(CDCException):
    pass

class ConfigurationException(CDCException):
    pass

class RetryableException(CDCException):
    def __init__(self, message: str, retry_after: int = 60, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after

class DataValidationException(CDCException):
    pass

class ConnectionException(CDCException):
    pass