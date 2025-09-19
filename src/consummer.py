from kafka import KafkaConsumer
import json
import logging
import base64
from decimal import Decimal

logger = logging.getLogger(__name__)

class CDCConsumer:
    def __init__(self, kafka_config):
        def safe_deserializer(m):
            if m is None:
                logger.debug("Message key/value is None")
                return None
            if isinstance(m, (dict, list)):
                logger.debug(f"Données déjà désérialisées: {m}")
                return m
            if not isinstance(m, (bytes, bytearray)):
                logger.warning(f"Type inattendu pour les données Kafka: {type(m)}. Retour des données telles quelles: {m}")
                return m
            try:
                return json.loads(m.decode('utf-8'))
            except Exception as e:
                logger.warning(f"Erreur de désérialisation JSON: {e}. Données brutes: {m[:100]}...")
                return None
            
        self.consumer = KafkaConsumer(
            *kafka_config['topics'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            group_id=kafka_config.get('group_id', 'cdc_python_consumer'),
            value_deserializer=safe_deserializer,
            key_deserializer=safe_deserializer,
            auto_offset_reset='earliest',
            enable_auto_commit=kafka_config.get('enable_auto_commit', True)
        )
        logger.info("Consumer Kafka initialisé avec désérialiseur sécurisé.")

    def consume(self):
        """Générateur qui yield les messages (topic, key, value) déjà désérialisés"""
        try:
            logger.info("Démarrage de la consommation des messages...")
            for message in self.consumer:
                topic = message.topic
                key = message.key  # Déjà désérialisé
                value = message.value  # Déjà désérialisé
                
                if key is None:
                    logger.warning(f"Clé manquante pour le message de {topic}")
                else:
                    key = key.get('payload', key)  # Extraire le payload si présent
                if value is None:
                    logger.warning(f"Valeur vide pour le message de {topic} (possible tombstone)")
                else:
                    value = value.get('payload', value)  # Extraire le payload si présent
                
                logger.debug(f"Message reçu de {topic} - Clé: {key}, Valeur: {value}")
                
                yield topic, key, value
                
        except Exception as e:
            logger.error(f"Erreur lors de la consommation: {e}", exc_info=True)
            raise
        finally:
            self.close()

    def close(self):
        if hasattr(self, 'consumer') and self.consumer:
            self.consumer.close()
            logger.info("Consumer Kafka fermé")