"""
Módulo para gestionar el almacenamiento y recuperación de operaciones.
Maneja la persistencia de operaciones en memoria y en disco.
"""

import json
import os
import uuid
import time
import threading
import logging
from enum import Enum

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OperationStatus(Enum):
    """Enumeración de estados posibles para una operación"""
    UNKNOWN = 0
    PENDING = 1
    PROCESSING = 2
    COMPLETED = 3
    FAILED = 4
    CANCELLED = 5

class OperationStore:
    """Clase para gestionar el almacenamiento y recuperación de operaciones"""
    
    def __init__(self, service_module=None):
        """
        Inicializa el almacén de operaciones
        
        Args:
            service_module: Módulo de servicio opcional que provee funciones específicas
        """
        self.service_module = service_module
        self.async_operations = {}
        self.operations_dir = self._init_operations_dir()
        
        # Cargar operaciones existentes
        self._load_operations()
    
    def _init_operations_dir(self):
        """
        Inicializa el directorio para almacenar operaciones
        
        Returns:
            str: Ruta al directorio de operaciones
        """
        # Por defecto, usar directorio "operations" en la ubicación actual
        operations_dir = os.path.join(os.getcwd(), "operations")
        
        # Si el servicio proporciona una ruta, usarla
        if self.service_module and hasattr(self.service_module, "OPERATIONS_DIR"):
            operations_dir = self.service_module.OPERATIONS_DIR
        
        # Crear directorio si no existe
        if not os.path.exists(operations_dir):
            try:
                os.makedirs(operations_dir)
                logger.info(f"Directorio de operaciones creado: {operations_dir}")
            except Exception as e:
                logger.error(f"Error al crear directorio de operaciones: {str(e)}")
        
        return operations_dir
    
    def _load_operations(self):
        """Carga operaciones desde archivos al diccionario en memoria"""
        if not os.path.exists(self.operations_dir):
            return
        
        for filename in os.listdir(self.operations_dir):
            if filename.endswith('.json'):
                try:
                    operation_id = filename[:-5]  # Quitar extensión .json
                    file_path = os.path.join(self.operations_dir, filename)
                    
                    with open(file_path, 'r') as f:
                        operation_data = json.load(f)
                        self.async_operations[operation_id] = operation_data
                        logger.info(f"Cargada operación: {operation_id}")
                except Exception as e:
                    logger.error(f"Error al cargar operación {filename}: {str(e)}")
    
    def save_operation(self, operation_id, operation_data):
        """
        Guarda una operación en un archivo JSON
        
        Args:
            operation_id: ID de la operación
            operation_data: Datos de la operación
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        try:
            file_path = os.path.join(self.operations_dir, f"{operation_id}.json")
            with open(file_path, 'w') as f:
                json.dump(operation_data, f)
            return True
        except Exception as e:
            logger.error(f"Error al guardar operación {operation_id}: {str(e)}")
            return False
    
    def get_operation(self, operation_id):
        """
        Obtiene una operación por su ID
        
        Args:
            operation_id: ID de la operación
            
        Returns:
            dict: Datos de la operación o None si no existe
        """
        # Verificar si la operación existe en memoria
        if operation_id in self.async_operations:
            return self.async_operations[operation_id]
        
        # Si no está en memoria, buscar en archivos
        file_path = os.path.join(self.operations_dir, f"{operation_id}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    operation_data = json.load(f)
                    # Cargar en memoria para futuros accesos
                    self.async_operations[operation_id] = operation_data
                    return operation_data
            except Exception as e:
                logger.error(f"Error al leer operación {operation_id}: {str(e)}")
        
        return None
    
    def register_pending_operation(self, operation_id, a, b):
        """
        Registra una operación como pendiente
        
        Args:
            operation_id: ID de la operación
            a: Primer operando
            b: Segundo operando
            
        Returns:
            dict: Datos de la operación registrada
        """
        op_data = {
            "status": OperationStatus.PENDING.value,
            "message": "Operación en cola",
            "a": a,
            "b": b,
            "timestamp": time.time()
        }
        
        # Guardar en memoria
        self.async_operations[operation_id] = op_data
        
        # Guardar en disco
        self.save_operation(operation_id, op_data)
        
        return op_data
    
    def process_operation(self, operation_id, a, b):
        """
        Procesa una operación y actualiza su estado
        
        Args:
            operation_id: ID de la operación
            a: Primer operando
            b: Segundo operando
            
        Returns:
            dict: Resultado de la operación
        """
        # Si hay un módulo de servicio que proporciona la función de procesamiento, usarla
        if self.service_module and hasattr(self.service_module, "process_async_operation"):
            result = self.service_module.process_async_operation(operation_id, a, b)
            # Convertir a diccionario si es necesario
            if hasattr(result, "__dict__"):
                result = result.__dict__
            return result
        
        # Implementación predeterminada
        try:
            # Marcar como en procesamiento
            self.update_operation_status(operation_id, OperationStatus.PROCESSING.value, "Procesando operación")
            
            # Simular tiempo de procesamiento
            time.sleep(1)
            
            # Realizar la operación
            result = a + b
            
            # Crear resultado
            result_dict = {
                "result": result,
                "success": True,
                "error_message": "",
                "operation_id": operation_id
            }
            
            # Marcar como completada
            self.update_operation_status(
                operation_id, 
                OperationStatus.COMPLETED.value, 
                "Operación completada", 
                result_dict
            )
            
            return result_dict
        
        except Exception as e:
            error_msg = f"Error al procesar: {str(e)}"
            logger.error(error_msg)
            
            # Marcar como fallida
            result_dict = {
                "result": 0,
                "success": False,
                "error_message": error_msg,
                "operation_id": operation_id
            }
            
            self.update_operation_status(
                operation_id, 
                OperationStatus.FAILED.value, 
                error_msg, 
                result_dict
            )
            
            return result_dict
    
    def update_operation_status(self, operation_id, status, message, result=None):
        """
        Actualiza el estado de una operación
        
        Args:
            operation_id: ID de la operación
            status: Nuevo estado (usar valores de OperationStatus)
            message: Mensaje descriptivo
            result: Resultado opcional
            
        Returns:
            dict: Datos actualizados de la operación
        """
        # Obtener operación actual
        operation = self.get_operation(operation_id) or {}
        
        # Actualizar estado y mensaje
        operation["status"] = status
        operation["message"] = message
        
        # Actualizar resultado si se proporciona
        if result:
            operation["result"] = result
        
        # Guardar cambios
        self.async_operations[operation_id] = operation
        self.save_operation(operation_id, operation)
        
        return operation
    
    def mark_as_failed(self, operation_id, error_message):
        """
        Marca una operación como fallida
        
        Args:
            operation_id: ID de la operación
            error_message: Mensaje de error
            
        Returns:
            dict: Datos actualizados de la operación
        """
        return self.update_operation_status(
            operation_id,
            OperationStatus.FAILED.value,
            f"Error al procesar: {error_message}"
        )
    
    def process_locally(self, a, b, operation_id=None):
        """
        Procesa una operación localmente (cuando MOM no está disponible)
        
        Args:
            a: Primer operando
            b: Segundo operando
            operation_id: ID opcional de la operación
            
        Returns:
            Tuple (operation_id, message): ID de la operación y mensaje de estado
        """
        # Generar ID si no se proporciona
        if not operation_id:
            operation_id = str(uuid.uuid4())
        
        # Registrar operación
        self.register_pending_operation(operation_id, a, b)
        
        # Procesar en un hilo separado para no bloquear
        thread = threading.Thread(
            target=lambda: self.process_operation(operation_id, a, b),
            daemon=True
        )
        thread.start()
        
        return operation_id, "Operación en proceso local (MOM no disponible)"
    
    def get_pending_operations(self):
        """
        Obtiene todas las operaciones pendientes
        
        Returns:
            dict: Diccionario de operaciones pendientes {id: datos}
        """
        pending_ops = {}
        
        # Buscar en memoria
        for op_id, op_data in self.async_operations.items():
            if op_data.get("status") == OperationStatus.PENDING.value:
                pending_ops[op_id] = op_data
        
        # Buscar en archivos (operaciones que no están en memoria)
        if os.path.exists(self.operations_dir):
            for filename in os.listdir(self.operations_dir):
                if not filename.endswith('.json'):
                    continue
                
                op_id = filename[:-5]  # Quitar extensión .json
                
                # Si ya está en nuestro diccionario, omitirla
                if op_id in pending_ops:
                    continue
                    
                try:
                    file_path = os.path.join(self.operations_dir, filename)
                    with open(file_path, 'r') as f:
                        operation_data = json.load(f)
                    
                    if operation_data.get("status") == OperationStatus.PENDING.value:
                        # Verificar que tenga los datos necesarios
                        if "a" in operation_data and "b" in operation_data:
                            pending_ops[op_id] = operation_data
                except Exception as e:
                    logger.error(f"Error al procesar archivo {filename}: {str(e)}")
        
        return pending_ops