import logging
from src.utils import load_config
from src.consummer import CDCConsumer
from src.postgres_writer import PostgresWriter

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Chargement de la config
        config = load_config()
        logger.info(f"Config postgres: {config.get('postgres', 'MANQUANT')}")
        logger.info("Configuration chargée avec succès")
        
        # Validation de la config
        if 'kafka' not in config or 'postgres' not in config:
            raise ValueError("Config incomplète: sections 'kafka' et 'postgres' requises")
        
        # Initialisation des composants
        consumer = CDCConsumer(config['kafka'])
        writer = PostgresWriter(config['postgres'])
        
        logger.info("Démarrage de la consommation des messages...")
        
        # Traitement des messages
        for topic, key, value in consumer.consume():
            try:
                # Extraction du nom de table depuis le topic
                if not topic.startswith("cdc.public."):
                    logger.warning(f"Topic format inattendu: {topic}")
                    continue
                    
                table = topic.replace("cdc.public.", "")
                
                # Gestion robuste de la clé primaire
                pk_field = None
                pk_value = None
                if key and isinstance(key, dict):
                    # Cas 1 : Clé avec payload (par exemple, {"payload": {"category_id": 1}})
                    if 'payload' in key and isinstance(key['payload'], dict):
                        payload_keys = list(key['payload'].keys())
                        if payload_keys:
                            pk_field = payload_keys[0]
                            pk_value = key['payload'].get(pk_field)
                    # Cas 2 : Clé sans payload (par exemple, {"category_id": 1})
                    else:
                        payload_keys = list(key.keys())
                        if payload_keys:
                            pk_field = payload_keys[0]
                            pk_value = key.get(pk_field)
                
                if not pk_field or pk_value is None:
                    logger.warning(f"Clé primaire manquante ou invalide pour {topic}: {key}")
                    continue

                logger.debug(f"Clé primaire détectée pour {topic}: {pk_field}={pk_value}")

                
                # Créer la table si elle n'existe pas
                writer.create_table_if_not_exists(table, key, value)

                # Gestion des suppressions
                operation = 'UPSERT'
                data = value
                
                if value is None or (value and value.get('__deleted') == 'true'):
                    logger.info(f"Suppression détectée pour {topic}, {pk_field}={key.get(pk_field)}")
                    operation = 'DELETE'
                    data = {pk_field: key.get(pk_field)}
                
                # Validation des données pour UPSERT
                elif not value:
                    logger.warning(f"Valeur vide pour {topic}")
                    continue
                
                # Écriture en base
                writer.write_data(table, data, pk_field, operation=operation)
                logger.debug(f"Données écrites pour {table} (opération: {operation})")
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement du message {topic}: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur fatale: {e}", exc_info=True)
        raise
    finally:
        # Nettoyage des ressources
        if 'consumer' in locals():
            consumer.close()
        if 'writer' in locals():
            writer.close()
        logger.info("Application fermée")

if __name__ == "__main__":
    main()