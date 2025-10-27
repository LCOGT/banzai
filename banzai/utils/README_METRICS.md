# OpenTelemetry Tracing Integration for Banzai


OpenTelemetry automatically creates spans for Celery tasks and provides decorators for instrumenting other functions. It also
supports imperatively creating spans and submitting metrics.

A collecting server is normally required to view Open Telemetry data. Two well known options are
[signoz](https://signoz.io/) and [Jaeger](https://www.jaegertracing.io/).


## Enabling Open Telemetry

### 1. Install The `otel` Dependency Group

```bash
poetry install --with otel
```

Banzai should still function without these dependencies installed. So you if you don't need tracing, don't
install them.

### 2. Enable Tracing

Set the environment variable to enable tracing:

```bash
export BANZAI_ENABLE_TRACING=true
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BANZAI_ENABLE_TRACING` | Enable/disable tracing | `false` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint for remote traces | Console export |

### Tracing of Celery Tasks

The celery instrumenter is set up to automatically create spans for celery tasks. It is wired up
in `celery.py`.

### Manual Tracing and Span Attributes

You can add tracing to any function using decorators. A good example
is the [download_from_s3](https://github.com/LCOGT/banzai/blob/3910c11eaeaa0c2035f051b99af0b49c75f2702b/banzai/utils/fits_utils.py#L71) function:

```python
from banzai.metrics import add_telemetry_span_attribute, trace_function

@trace_function("download_from_s3")
def download_from_s3(file_info, context, is_raw_frame=False):
    frame_id = file_info.get('frameid')
    add_telemetry_span_attribute('frame_id', frame_id)
    add_telemetry_span_attribute('frame_filename', file_info.get('filename'))
```
