"""Custom exceptions for pipeline domains."""
class BaseProcessingException(Exception):
    pass

class MetadataValidationError(BaseProcessingException):
    pass

class S3ReadError(BaseProcessingException):
    pass

class DDBWriteError(BaseProcessingException):
    pass

class KBSyncError(BaseProcessingException):
    pass

class ExpiredTokenException(BaseProcessingException):
    pass

class NotReadyException(BaseProcessingException):
    pass
