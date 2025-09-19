# Kafka CDC PostgreSQL Sink

A robust, production-ready Change Data Capture (CDC) sink that consumes Debezium events from Kafka and synchronizes them to PostgreSQL.

## Features

- **High Performance**: Multi-threaded processing with configurable worker pools
- **Reliability**: Automatic retry logic, error handling, and graceful shutdown
- **Monitoring**: Built-in metrics (Prometheus format) and health checks
- **Type Safety**: Intelligent Debezium type mapping to PostgreSQL
- **Auto Schema**: Automatic table creation based on CDC events
- **Batch Processing**: Efficient batch writes to PostgreSQL
- **Configuration**: Flexible YAML/JSON configuration with environment variable overrides

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│    Kafka    │────>│  Consumer    │────>│   Worker     │
│   Topics    │     │   Thread     │     │   Threads    │
└─────────────┘     └──────────────┘     └──────────────┘
                            │                     │
                            ▼                     ▼
                     ┌──────────────┐     ┌──────────────┐
                     │    Queue     │────>│  PostgreSQL  │
                     └──────────────┘     │    Writer    │
                                          └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │   Metrics    │
                     │   Server     │
                     └──────────────┘
```

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/kafka-cdc-sink.git
cd kafka-cdc-sink

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/
```

## Configuration

Create a `config/config.yaml` file:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    - "cdc.public.users"
    - "cdc.public.orders"
  group_id: "cdc_consumer"
  batch_size: 100
  enable_auto_commit: false

postgres:
  host: "localhost"
  port: 5432
  database: "target_db"
  user: "postgres"
  password: "password"
  max_connections: 10

logging:
  level: "INFO"
  file: "logs/cdc.log"

metrics:
  enabled: true
  port: 8080

processing:
  max_workers: 4
  queue_size: 1000
```

### Environment Variables

Override configuration with environment variables:

- `CDC_KAFKA_BOOTSTRAP_SERVERS`
- `CDC_KAFKA_GROUP_ID`
- `CDC_POSTGRES_HOST`
- `CDC_POSTGRES_PORT`
- `CDC_POSTGRES_DATABASE`
- `CDC_POSTGRES_USER`
- `CDC_POSTGRES_PASSWORD`
- `CDC_LOG_LEVEL`
- `CDC_METRICS_ENABLED`
- `CDC_METRICS_PORT`

## Usage

### Running the Application

```bash
# Using the main script
python main.py

# Or make it executable
chmod +x main.py
./main.py

# With custom config
python main.py --config /path/to/config.yaml
```

### Docker

```bash
# Build the image
docker build -t kafka-cdc-sink .

# Run the container
docker run -d \
  --name cdc-sink \
  -v $(pwd)/config:/app/config \
  -p 8080:8080 \
  --network="host" \
  kafka-cdc-sink
```

### Docker Compose

```yaml
version: '3.8'

services:
  cdc-sink:
    build: .
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    ports:
      - "8080:8080"
    environment:
      - CDC_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - CDC_POSTGRES_HOST=postgres
    depends_on:
      - kafka
      - postgres
    restart: unless-stopped
```

## Monitoring

### Health Check

```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00Z",
  "components": {
    "consumer": {
      "status": "healthy",
      "topics": ["cdc.public.users"],
      "partitions": 3,
      "lag": 0
    },
    "writer": {
      "status": "healthy",
      "database": "target_db",
      "connection": "ok",
      "pool_size": 10
    }
  }
}
```

### Metrics (Prometheus format)

```bash
curl http://localhost:8080/metrics
```

Available metrics:
- `uptime_seconds`: Application uptime
- `consumer_messages_received`: Total messages consumed
- `consumer_commits`: Successful offset commits
- `writer_inserts/updates/deletes`: Database operations
- `processor_batch_processing_time`: Processing latency
- `processor_queue_size`: Current queue depth
- `error_handler_*`: Error statistics

### Status Endpoint

```bash
curl http://localhost:8080/status
```

## Type Mappings

| Debezium Type | PostgreSQL Type |
|--------------|----------------|
| int32 | INTEGER |
| int64 | BIGINT |
| float32 | REAL |
| float64 | DOUBLE PRECISION |
| boolean | BOOLEAN |
| string | VARCHAR(255) / TEXT |
| bytes | BYTEA |
| io.debezium.time.Date | DATE |
| io.debezium.time.Timestamp | TIMESTAMP |
| io.debezium.time.MicroTimestamp | TIMESTAMP |
| io.debezium.data.Json | JSONB |
| io.debezium.data.Uuid | UUID |

### Special Field Handling

- `price`, `amount`, `total`: Decoded from base64 to DECIMAL
- `*_at` fields: Converted from microseconds to TIMESTAMP
- `__deleted`: String to BOOLEAN conversion
- JSON objects/arrays: Serialized to JSONB

## Development

### Project Structure

```
kafka-cdc-sink/
├── src/
│   ├── core/           # Base classes and exceptions
│   ├── consumers/      # Kafka consumer implementation
│   ├── writers/        # PostgreSQL writer
│   ├── handlers/       # Main processor logic
│   ├── schemas/        # Type mapping and validation
│   ├── config/         # Configuration management
│   ├── monitoring/     # Metrics and health checks
│   └── utils/          # Utility functions
├── tests/
│   ├── unit/          # Unit tests
│   ├── integration/   # Integration tests
│   └── fixtures/      # Test data
├── config/
│   └── config.yaml    # Configuration file
├── logs/              # Application logs
├── scripts/           # Utility scripts
├── main.py           # Entry point
├── requirements.txt  # Dependencies
└── README.md        # Documentation
```

### Running Tests

```bash
# All tests
python -m pytest

# Unit tests only
python -m pytest tests/unit/

# With coverage
python -m pytest --cov=src tests/

# Specific test
python -m pytest tests/unit/test_type_mapper.py
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run linting: `flake8 src/`
6. Submit a pull request

## Performance Tuning

### Kafka Consumer
- Adjust `batch_size` for throughput vs latency
- Tune `session_timeout_ms` for rebalancing behavior
- Configure `max_poll_interval_ms` for slow processing

### PostgreSQL Writer
- Increase `max_connections` for parallelism
- Adjust `batch.size` for write efficiency
- Enable `continue_on_error` for resilience

### Processing
- Scale `max_workers` based on CPU cores
- Increase `queue_size` for burst handling
- Tune `shutdown_timeout` for graceful stops

## Troubleshooting

### Common Issues

1. **High Consumer Lag**
   - Increase worker threads
   - Optimize batch sizes
   - Check PostgreSQL performance

2. **Connection Pool Exhaustion**
   - Increase max_connections
   - Check for connection leaks
   - Monitor transaction duration

3. **Memory Usage**
   - Reduce queue_size
   - Decrease batch sizes
   - Enable memory profiling

### Debug Mode

Enable debug logging:
```yaml
logging:
  level: "DEBUG"
```

## License

MIT License - see LICENSE file for details

## Support

- Issues: GitHub Issues
- Documentation: Wiki
- Contact: support@example.com