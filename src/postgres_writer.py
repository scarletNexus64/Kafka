import psycopg2
import logging
import base64
from decimal import Decimal

logger = logging.getLogger(__name__)

class PostgresWriter:
    def __init__(self, config):
        try:
            self.connection = psycopg2.connect(
                dbname=config.get('database') or config.get('dbname'),
                user=config['user'],
                password=config['password'],
                host=config['host'],
                port=config.get('port', 5432)
            )
            self.cursor = self.connection.cursor()
            self.created_tables = set()  # Cache pour les tables créées
            logger.info("Connexion PostgreSQL établie")
        except Exception as e:
            logger.error(f"Erreur connexion PostgreSQL: {e}")
            raise

    def _process_data_values(self, data):
        """Convertit les valeurs pour PostgreSQL, notamment les champs DECIMAL encodés en base64"""
        processed_data = {}
        for key, value in data.items():
            if key == 'price' and isinstance(value, str):  # Gérer le champ price encodé en base64
                try:
                    if value:
                        decoded_bytes = base64.b64decode(value)
                        processed_data[key] = Decimal(decoded_bytes.hex()) / 100  # Échelle de 2
                    else:
                        processed_data[key] = None
                except Exception as e:
                    logger.warning(f"Erreur lors du décodage du champ price: {e}, valeur: {value}")
                    processed_data[key] = None
            elif key == '__deleted':  # Convertir la chaîne 'true'/'false' en booléen
                processed_data[key] = value == 'true'
            elif key == 'created_at' and isinstance(value, int):  # Convertir MicroTimestamp en timestamp
                processed_data[key] = value / 1000000  # Convertir microsecondes en secondes
            else:
                processed_data[key] = value
        return processed_data

    def write_data(self, table, data, pk_field, operation='UPSERT'):
        """Écrit les données dans la table spécifiée, gère UPSERT et DELETE"""
        try:
            if operation == 'DELETE':
                if not pk_field or not data.get(pk_field):
                    raise ValueError(f"Champ de clé primaire {pk_field} manquant pour suppression")
                sql = f"DELETE FROM {table} WHERE {pk_field} = %s"
                self.cursor.execute(sql, (data.get(pk_field),))
                self.connection.commit()
                logger.debug(f"Données supprimées de {table} pour {pk_field}={data.get(pk_field)}")
                return

            if not data or not isinstance(data, dict):
                raise ValueError("Les données doivent être un dictionnaire non vide pour UPSERT")
            
            if not table or not pk_field:
                raise ValueError("Table et pk_field sont requis pour UPSERT")

            # Conversion des valeurs pour PostgreSQL
            processed_data = self._process_data_values(data)

            columns = ', '.join(processed_data.keys())
            values = ', '.join(['%s'] * len(processed_data))
            update_set = ', '.join([f"{col}=EXCLUDED.{col}" for col in processed_data.keys()])
            
            sql = f"""
                INSERT INTO {table} ({columns})
                VALUES ({values})
                ON CONFLICT ({pk_field}) DO UPDATE SET {update_set}
            """
            
            self.cursor.execute(sql, list(processed_data.values()))
            self.connection.commit()
            
            logger.debug(f"Données insérées/mises à jour dans {table} pour {pk_field}={data.get(pk_field)}")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Erreur lors de l'écriture dans {table}: {e}")
            raise


    def _map_debezium_type_to_postgres(self, field_type, field_name):
        """Mappe les types Debezium aux types PostgreSQL"""
        if field_type == 'int32':
            return 'NUMERIC'
        elif field_type == 'int64':
            return 'NUMERIC'
        elif field_type == 'string':
            return 'VARCHAR(255)' if field_name != 'description' else 'CHARACTER VARYING'
        elif field_type == 'bytes' and field_name in ('price', 'amount', 'total'):
            return 'NUMERIC'
        elif field_type == 'io.debezium.time.MicroTimestamp':
            return 'TIMESTAMP'
        elif field_type == 'boolean':
            return 'BOOLEAN'
        else:
            logger.warning(f"Type Debezium inconnu: {field_type} pour {field_name}, utilisation de CHARACTER VARYING")
            return 'CHARACTER VARYING'


    def _table_exists(self, table):
        """Vérifie si la table existe dans le schéma public"""
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table,))
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Erreur lors de la vérification de l'existence de la table {table}: {e}")
            raise

    def _get_column_definition(self, field_name, field_value, pk_field=None):
        if field_name == pk_field:
            return None  # Éviter d'ajouter la clé primaire deux fois
        if field_name == '__deleted':
            return f"{field_name} BOOLEAN"
        if field_name.endswith('_at'):
            return f"{field_name} TIMESTAMP"
        if field_name in ('price', 'amount', 'total'):
            return f"{field_name} NUMERIC"
        if isinstance(field_value, int):
            return f"{field_name} NUMERIC"
        if isinstance(field_value, str):
            if field_name == 'description':
                return f"{field_name} CHARACTER VARYING"
            return f"{field_name} VARCHAR(255)"
        return f"{field_name} CHARACTER VARYING"
    
    
    def _extract_pk_field(self, key):
        """Extrait le champ de la clé primaire depuis key"""
        if key and isinstance(key, dict):
            if 'payload' in key and isinstance(key['payload'], dict):
                return list(key['payload'].keys())[0] if key['payload'] else None
            return list(key.keys())[0] if key else None
        return None

    def _extract_table_structure(self, key, value):
        columns = []
        pk_field = None
        pk_field = self._extract_pk_field(key)
        if pk_field:
            columns.append(f"{pk_field} NUMERIC PRIMARY KEY")
        if value and isinstance(value, dict):
            for field_name, field_value in value.items():
                columns.append(self._get_column_definition(field_name, field_value,pk_field))
        return pk_field, [col for col in columns if col]

    

    def create_table_if_not_exists(self, table, key, value):
        """Crée la table si elle n'existe pas, basée sur les données du message CDC"""
        try:
            if self._table_exists(table):
                logger.debug(f"La table {table} existe déjà")
                return

            _, columns = self._extract_table_structure(key, value)
            if not columns:
                logger.warning(f"Aucune colonne définie pour la table {table}, création impossible")
                return

            columns_sql = ', '.join(columns)
            create_table_sql = f"CREATE TABLE public.{table} ({columns_sql})"
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info(f"Table {table} créée avec succès")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Erreur lors de la création de la table {table}: {e}")
            raise

    def close(self):
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
            logger.info("Connexion PostgreSQL fermée")
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture: {e}")