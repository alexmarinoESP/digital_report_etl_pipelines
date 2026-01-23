"""Custom exception hierarchy for the social module.

This module defines a clear exception hierarchy that makes error handling
more precise and allows for better error recovery strategies.
"""

from typing import Optional, Dict, Any


class SocialError(Exception):
    """Base exception for all social module errors.

    All custom exceptions in the social module inherit from this base class,
    making it easy to catch all social-related errors if needed.
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize the exception.

        Args:
            message: Human-readable error message
            details: Optional dictionary with additional error context
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        """Return string representation of the error."""
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class AuthenticationError(SocialError):
    """Raised when authentication or token operations fail.

    Examples:
        - Token expired and refresh failed
        - Invalid credentials
        - Token not found in database
    """

    pass


class APIError(SocialError):
    """Raised when API requests fail.

    Examples:
        - HTTP 4xx/5xx errors
        - Network timeouts
        - Invalid API responses
        - Rate limiting
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Initialize API error with HTTP details.

        Args:
            message: Human-readable error message
            status_code: HTTP status code
            response_body: Raw response body
            details: Optional additional context
        """
        super().__init__(message, details)
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self) -> str:
        """Return string representation including HTTP status."""
        base = self.message
        if self.status_code:
            base = f"[HTTP {self.status_code}] {base}"
        if self.response_body:
            base = f"{base}\nResponse: {self.response_body[:500]}"  # Truncate long responses
        if self.details:
            base = f"{base}\nDetails: {self.details}"
        return base


class ConfigurationError(SocialError):
    """Raised when configuration is invalid or missing.

    Examples:
        - Missing required configuration keys
        - Invalid configuration values
        - Configuration file not found
        - Schema validation errors
    """

    pass


class DataValidationError(SocialError):
    """Raised when data validation fails.

    Examples:
        - Missing required fields
        - Invalid data types
        - Schema mismatches
        - Data integrity violations
    """

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        expected: Optional[Any] = None,
        actual: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Initialize data validation error with field details.

        Args:
            message: Human-readable error message
            field: Field name that failed validation
            expected: Expected value or type
            actual: Actual value or type
            details: Optional additional context
        """
        super().__init__(message, details)
        self.field = field
        self.expected = expected
        self.actual = actual

    def __str__(self) -> str:
        """Return string representation with validation details."""
        base = self.message
        if self.field:
            base = f"{base} (field: {self.field})"
        if self.expected is not None and self.actual is not None:
            base = f"{base} - Expected: {self.expected}, Got: {self.actual}"
        if self.details:
            base = f"{base}\nDetails: {self.details}"
        return base


class DatabaseError(SocialError):
    """Raised when database operations fail.

    Examples:
        - Connection failures
        - SQL syntax errors
        - Constraint violations
        - Transaction rollbacks
    """

    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Initialize database error with query context.

        Args:
            message: Human-readable error message
            query: SQL query that caused the error
            details: Optional additional context
        """
        super().__init__(message, details)
        self.query = query

    def __str__(self) -> str:
        """Return string representation with query."""
        base = self.message
        if self.query:
            base = f"{base}\nQuery: {self.query[:500]}"  # Truncate long queries
        if self.details:
            base = f"{base}\nDetails: {self.details}"
        return base


class RetryableError(SocialError):
    """Raised when an operation can be retried.

    This exception indicates that the operation failed but may succeed
    if retried (e.g., temporary network issues, rate limiting).
    """

    def __init__(
        self,
        message: str,
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Initialize retryable error.

        Args:
            message: Human-readable error message
            retry_after: Seconds to wait before retry
            details: Optional additional context
        """
        super().__init__(message, details)
        self.retry_after = retry_after


class PlatformNotSupportedError(SocialError):
    """Raised when an unsupported platform is requested.

    Examples:
        - Requesting data from 'twitter' when only LinkedIn/Google implemented
    """

    pass


class PipelineError(SocialError):
    """Raised when a pipeline execution fails.

    Examples:
        - Pipeline configuration error
        - Data extraction failure
        - Data transformation failure
        - Data load failure
    """

    def __init__(
        self,
        message: str,
        pipeline_name: Optional[str] = None,
        stage: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """Initialize pipeline error.

        Args:
            message: Human-readable error message
            pipeline_name: Name of the pipeline that failed
            stage: Pipeline stage where error occurred (extract/transform/load)
            details: Optional additional context
        """
        super().__init__(message, details)
        self.pipeline_name = pipeline_name
        self.stage = stage

    def __str__(self) -> str:
        """Return string representation with pipeline context."""
        base = self.message
        if self.pipeline_name:
            base = f"[{self.pipeline_name}] {base}"
        if self.stage:
            base = f"{base} (stage: {self.stage})"
        if self.details:
            base = f"{base}\nDetails: {self.details}"
        return base
