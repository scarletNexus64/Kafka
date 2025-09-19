#!/usr/bin/env python3
import logging
import signal
import sys
from pathlib import Path

from src.config.config_loader import ConfigLoader
from src.handlers.cdc_processor import CDCProcessor
from src.monitoring.metrics import MetricsCollector
from flask import Flask, jsonify, Response
import threading

def setup_logging(config: dict):
    log_config = config.get('logging', {})
    
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_config.get('file'):
        log_dir = Path(log_config['file']).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_config['file']))
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers
    )

def create_metrics_server(processor: CDCProcessor, config: dict):
    app = Flask(__name__)
    app.logger.disabled = True
    logging.getLogger('werkzeug').disabled = True
    
    @app.route('/health')
    def health():
        status = processor.get_status()
        health_status = status['health']['status']
        
        if health_status == 'healthy':
            return jsonify(status['health']), 200
        elif health_status == 'degraded':
            return jsonify(status['health']), 206
        else:
            return jsonify(status['health']), 503
    
    @app.route('/metrics')
    def metrics():
        return Response(
            processor.metrics.export_prometheus(),
            mimetype='text/plain'
        )
    
    @app.route('/status')
    def status():
        return jsonify(processor.get_status())
    
    return app

def main():
    try:
        config_loader = ConfigLoader()
        config = config_loader.load()
        
        setup_logging(config)
        
        logger = logging.getLogger('main')
        logger.info("Starting Kafka CDC Sink Application")
        logger.info(f"Configuration loaded from: {config_loader.config_path}")
        
        processor = CDCProcessor(config)
        
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            processor.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        if config.get('metrics', {}).get('enabled', True):
            metrics_port = config['metrics'].get('port', 8080)
            app = create_metrics_server(processor, config)
            
            metrics_thread = threading.Thread(
                target=lambda: app.run(host='0.0.0.0', port=metrics_port, debug=False),
                daemon=True
            )
            metrics_thread.start()
            logger.info(f"Metrics server started on port {metrics_port}")
        
        processor.start()
        
        while True:
            signal.pause()
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        processor.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()