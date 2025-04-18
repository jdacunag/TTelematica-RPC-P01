import pymongo
from pymongo import MongoClient
import time
import logging
import os
from bson.objectid import ObjectId
import dns.resolver  # Para manejar las conexiones SRV

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Obtener URI directamente desde variable de entorno o usar la que sabemos que funciona
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb+srv://admin:admin123@telematica-rpc.mxzpwyj.mongodb.net/microservices_db?retryWrites=true&w=majority')
MONGO_DB = os.environ.get('MONGO_DB', 'microservices_db')
MONGO_COLLECTION = os.environ.get('MONGO_COLLECTION', 'operations')

class OperationsDB:
    """Clase para gestionar operaciones en MongoDB Atlas"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Singleton para asegurar una sola conexión a MongoDB"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Inicializa la conexión a MongoDB Atlas"""
        self.client = None
        self.db = None
        self.collection = None
        self._connect()
        
    def _connect(self):
        """Establece conexión con MongoDB Atlas"""
        try:
            # Registrar debug info
            logger.info(f"Intentando conectar a MongoDB Atlas: {MONGO_DB}")
            logger.info(f"URI: {MONGO_URI.split('@')[0].split('://')[0]}://*****@{MONGO_URI.split('@')[1] if '@' in MONGO_URI else 'no-uri'}")
            
            # Configurar timeouts y opciones de conexión adecuados para la nube
            connection_options = {
                'connectTimeoutMS': 30000,
                'socketTimeoutMS': 30000,
                'serverSelectionTimeoutMS': 30000,
                'retryWrites': True,
                'retryReads': True
            }
            
            # Crear conexión con MongoDB Atlas
            self.client = MongoClient(MONGO_URI, **connection_options)
            
            # Verificar conexión con ping
            self.client.admin.command('ping')
            
            # Seleccionar base de datos y colección
            self.db = self.client[MONGO_DB]
            self.collection = self.db[MONGO_COLLECTION]
            
            # Crear índices
            self.collection.create_index([("operation_id", pymongo.ASCENDING)], unique=True)
            self.collection.create_index([("service", pymongo.ASCENDING)])
            self.collection.create_index([("timestamp", pymongo.ASCENDING)])
            self.collection.create_index([("status", pymongo.ASCENDING)])
            
            logger.info(f"Conexión exitosa a MongoDB Atlas: {MONGO_DB}")
            return True
        except Exception as e:
            logger.error(f"Error al conectar a MongoDB Atlas: {str(e)}")
            return False
    
    def is_connected(self):
        """Verifica si la conexión a MongoDB Atlas está activa"""
        if self.client is None:
            return False
        try:
            # Ping para verificar conexión
            self.client.admin.command('ping')
            return True
        except Exception:
            return False
    
    def reconnect(self):
        """Reconecta a MongoDB Atlas si la conexión se perdió"""
        if not self.is_connected():
            logger.info("Reconectando a MongoDB Atlas...")
            return self._connect()
        return True
    
    def save_operation(self, operation_id, operation_data):
        """
        Guarda una operación en MongoDB Atlas
        
        Args:
            operation_id: ID de la operación
            operation_data: Datos de la operación
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        if not self.reconnect():
            logger.error("No se pudo guardar la operación: sin conexión a MongoDB Atlas")
            return False
        
        try:
            # Asegurar que el operation_id esté incluido en los datos
            operation_data['operation_id'] = operation_id
            
            # Verificar si la operación ya existe
            existing = self.collection.find_one({"operation_id": operation_id})
            
            if existing:
                # Actualizar operación existente
                result = self.collection.update_one(
                    {"operation_id": operation_id},
                    {"$set": operation_data}
                )
                success = result.modified_count > 0
            else:
                # Insertar nueva operación
                result = self.collection.insert_one(operation_data)
                success = result.inserted_id is not None
            
            if success:
                logger.info(f"Operación {operation_id} guardada en MongoDB Atlas")
            else:
                logger.warning(f"No se pudo guardar la operación {operation_id} en MongoDB Atlas")
            
            return success
        except Exception as e:
            logger.error(f"Error al guardar operación {operation_id} en MongoDB Atlas: {str(e)}")
            return False
    
    def get_operation(self, operation_id):
        """
        Obtiene una operación por su ID
        
        Args:
            operation_id: ID de la operación
            
        Returns:
            dict: Datos de la operación o None si no existe
        """
        if not self.reconnect():
            logger.error("No se pudo recuperar la operación: sin conexión a MongoDB Atlas")
            return None
        
        try:
            operation = self.collection.find_one({"operation_id": operation_id})
            
            if operation:
                # Convertir ObjectId a string para serialización JSON
                if '_id' in operation:
                    operation['_id'] = str(operation['_id'])
                
                logger.info(f"Operación {operation_id} recuperada de MongoDB Atlas")
            else:
                logger.info(f"Operación {operation_id} no encontrada en MongoDB Atlas")
            
            return operation
        except Exception as e:
            logger.error(f"Error al recuperar operación {operation_id} de MongoDB Atlas: {str(e)}")
            return None
    
    def get_pending_operations(self, service=None):
        """
        Obtiene todas las operaciones pendientes
        
        Args:
            service: Filtrar por servicio (opcional)
            
        Returns:
            dict: Diccionario de operaciones pendientes {id: datos}
        """
        if not self.reconnect():
            logger.error("No se pudieron recuperar operaciones pendientes: sin conexión a MongoDB Atlas")
            return {}
        
        try:
            # Construir filtro
            query = {"status": 1}  # PENDING
            if service:
                query["service"] = service
            
            # Ejecutar consulta
            cursor = self.collection.find(query)
            
            # Convertir a diccionario
            pending_ops = {}
            for op in cursor:
                if '_id' in op:
                    op['_id'] = str(op['_id'])
                pending_ops[op['operation_id']] = op
            
            logger.info(f"Recuperadas {len(pending_ops)} operaciones pendientes de MongoDB Atlas")
            return pending_ops
        except Exception as e:
            logger.error(f"Error al recuperar operaciones pendientes de MongoDB Atlas: {str(e)}")
            return {}
    
    def list_operations(self, service=None, limit=100, skip=0, sort_by='timestamp', sort_dir=-1):
        """
        Lista operaciones con filtros y paginación
        
        Args:
            service: Filtro por servicio
            limit: Límite de resultados
            skip: Número de resultados a omitir
            sort_by: Campo para ordenar
            sort_dir: Dirección de ordenamiento (1=ascendente, -1=descendente)
            
        Returns:
            list: Lista de operaciones
        """
        if not self.reconnect():
            logger.error("No se pudieron listar operaciones: sin conexión a MongoDB Atlas")
            return []
        
        try:
            # Construir filtro
            query = {}
            if service:
                query["service"] = service
            
            # Ejecutar consulta con ordenamiento y paginación
            cursor = self.collection.find(
                query,
                sort=[(sort_by, sort_dir)],
                skip=skip,
                limit=limit
            )
            
            # Convertir a lista
            operations = []
            for op in cursor:
                if '_id' in op:
                    op['_id'] = str(op['_id'])
                operations.append(op)
            
            logger.info(f"Recuperadas {len(operations)} operaciones de MongoDB Atlas")
            return operations
        except Exception as e:
            logger.error(f"Error al listar operaciones de MongoDB Atlas: {str(e)}")
            return []
    
    def count_operations(self, service=None):
        """
        Cuenta el número total de operaciones
        
        Args:
            service: Filtro por servicio
            
        Returns:
            int: Número de operaciones
        """
        if not self.reconnect():
            logger.error("No se pudieron contar operaciones: sin conexión a MongoDB Atlas")
            return 0
        
        try:
            # Construir filtro
            query = {}
            if service:
                query["service"] = service
            
            # Contar documentos
            count = self.collection.count_documents(query)
            
            logger.info(f"Contadas {count} operaciones en MongoDB Atlas")
            return count
        except Exception as e:
            logger.error(f"Error al contar operaciones en MongoDB Atlas: {str(e)}")
            return 0
    
    def delete_operation(self, operation_id):
        """
        Elimina una operación
        
        Args:
            operation_id: ID de la operación
            
        Returns:
            bool: True si se eliminó correctamente
        """
        if not self.reconnect():
            logger.error("No se pudo eliminar la operación: sin conexión a MongoDB Atlas")
            return False
        
        try:
            result = self.collection.delete_one({"operation_id": operation_id})
            success = result.deleted_count > 0
            
            if success:
                logger.info(f"Operación {operation_id} eliminada de MongoDB Atlas")
            else:
                logger.warning(f"No se pudo eliminar la operación {operation_id} de MongoDB Atlas")
            
            return success
        except Exception as e:
            logger.error(f"Error al eliminar operación {operation_id} de MongoDB Atlas: {str(e)}")
            return False
    
    def close(self):
        """Cierra la conexión a MongoDB Atlas"""
        if self.client:
            try:
                self.client.close()
                logger.info("Conexión a MongoDB Atlas cerrada")
            except Exception as e:
                logger.error(f"Error al cerrar conexión a MongoDB Atlas: {str(e)}")