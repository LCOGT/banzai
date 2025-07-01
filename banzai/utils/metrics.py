"""
OpenTelemetry Tracing for Banzai.

See https://opentelemetry.io/docs/instrumentation/python/ for official documentation.

Tracing Environment Variables
- BANZAI_ENABLE_TRACING: Enable tracing if set to 'true', '1', or 'yes'.
- OTEL_EXPORTER_OTLP_ENDPOINT: The endpoint for exporting traces.
"""

import os
import functools
import logging
from typing import Optional, Any, Callable

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.trace import Status, StatusCode
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    # Create placeholder classes for when OpenTelemetry is not available
    trace = None

logger = logging.getLogger(__name__)


class TracingManager:
    def __init__(self):
        self._tracer = None
        self._enabled = OPENTELEMETRY_AVAILABLE and self._should_enable_tracing()

        if self._enabled:
            self._setup_tracing()

    def _should_enable_tracing(self) -> bool:
        """Check if tracing should be enabled based on environment variables."""
        return os.getenv('BANZAI_ENABLE_TRACING', 'false').lower() in ['true', '1', 'yes']

    def _setup_tracing(self):
        """Initialize OpenTelemetry tracing."""
        try:
            # Create resource with service information
            resource = Resource.create({
                "service.name": "banzai-pipeline",
                "service.version": '1.0.0', # Possible populate this will Banzi version?
            })

            # Create tracer provider
            provider = TracerProvider(resource=resource)

            # Setup span exporter - use OTLP if endpoint is configured, otherwise console
            otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT')
            if otlp_endpoint:
                exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
                logger.warning(f"OpenTelemetry tracing configured with OTLP endpoint: {otlp_endpoint}")
            else:
                exporter = ConsoleSpanExporter()
                logger.error("OpenTelemetry tracing configured with console exporter")

            # Add span processor
            span_processor = BatchSpanProcessor(exporter)
            provider.add_span_processor(span_processor)

            # Set the global tracer provider
            trace.set_tracer_provider(provider)

            # Get tracer
            self._tracer = trace.get_tracer(__name__)

            logger.info("OpenTelemetry tracing initialized successfully")

        except Exception as e:
            logger.warning(f"Failed to initialize OpenTelemetry tracing: {e}")
            self._enabled = False

    def get_tracer(self):
        """Get the configured tracer."""
        return self._tracer if self._enabled else None

    def is_enabled(self) -> bool:
        """Check if tracing is enabled."""
        return self._enabled


# Global tracing manager instance
_tracing_manager = None


def get_tracing_manager() -> TracingManager:
    """Get the global tracing manager instance."""
    global _tracing_manager
    if _tracing_manager is None:
        _tracing_manager = TracingManager()
    return _tracing_manager


def trace_function(
    span_name: Optional[str] = None,
    attributes: Optional[dict] = None
) -> Callable:
    """
    Decorator to add OpenTelemetry tracing to a function.

    @trace_function(span_name="custom_operation", attributes={"key": "value"})
    def another_function():
        pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            manager = get_tracing_manager()
            tracer = manager.get_tracer()

            if not tracer:
                # Tracing is disabled, just call the function
                return func(*args, **kwargs)

            # Determine span name
            name = span_name or f"{func.__module__}.{func.__name__}"

            # Start span
            with tracer.start_as_current_span(name) as span:
                try:
                    # Add custom attributes
                    if attributes:
                        for key, value in attributes.items():
                            span.set_attribute(key, str(value))

                    # Add function information
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)

                    # Add argument information
                    if args:
                        span.set_attribute("function.args_count", len(args))
                    if kwargs:
                        span.set_attribute("function.kwargs_count", len(kwargs))
                        for key, value in kwargs.items():
                            span.set_attribute(f"function.kwarg.{key}", str(value)[:100])  # Limit length

                    # Execute function
                    result = func(*args, **kwargs)

                    # Mark span as successful
                    span.set_status(Status(StatusCode.OK))

                    return result

                except Exception as e:
                    # Record exception in span
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator


def add_span_attribute(key: str, value: Any):
    """
    Add an attribute to the current active span.
    value will be converted to string using str()
    """
    manager = get_tracing_manager()
    if not manager.is_enabled():
        return

    try:
        current_span = trace.get_current_span()
        if current_span and current_span.is_recording():
            current_span.set_attribute(key, str(value))
    except Exception:
        # Silently ignore errors to avoid breaking the application
        pass


def add_span_event(name: str, attributes: Optional[dict] = None):
    """
    Add an event to the current active span.

    """
    manager = get_tracing_manager()
    if not manager.is_enabled():
        return

    try:
        current_span = trace.get_current_span()
        if current_span and current_span.is_recording():
            event_attributes = {}
            if attributes:
                event_attributes = {k: str(v) for k, v in attributes.items()}
            current_span.add_event(name, event_attributes)
    except Exception:
        # Silently ignore errors to avoid breaking the application
        pass


def create_manual_span(span_name: str, attributes: Optional[dict] = None):
    """
    Create a manual span context manager.

    """
    manager = get_tracing_manager()
    tracer = manager.get_tracer()

    if not tracer:
        # Return a no-op context manager
        from contextlib import nullcontext
        return nullcontext()

    span = tracer.start_span(span_name)

    if attributes:
        for key, value in attributes.items():
            span.set_attribute(key, str(value))

    return span
