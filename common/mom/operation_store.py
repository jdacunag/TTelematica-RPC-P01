"""
Módulo para gestionar el almacenamiento y recuperación de operaciones.
Maneja la persistencia de operaciones en memoria y en MongoDB Atlas.
"""

import json
import os
import uuid
import time
import threading
import logging
from enum import Enum

# Importar la clase OperationsDB
from common.db.operations_db import OperationsDB

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
        self.db = OperationsDB.get_instance()
        
        # Cargar operaciones existentes
        self._load_operations()
    
    def _load_operations(self):
        """Carga operaciones pendientes desde MongoDB Atlas al diccionario en memoria"""
        try:
            # Obtener todas las operaciones pendientes de MongoDB Atlas
            pending_ops = self.db.get_pending_operations()
            
            # Cargar en memoria
            for op_id, op_data in pending_ops.items():
                self.async_operations[op_id] = op_data
                logger.info(f"Cargada operación pendiente desde MongoDB Atlas: {op_id}")
        except Exception as e:
            logger.error(f"Error al cargar operaciones desde MongoDB Atlas: {str(e)}")
    
    def save_operation(self, operation_id, operation_data):
        """
        Guarda una operación en MongoDB Atlas
        
        Args:
            operation_id: ID de la operación
            operation_data: Datos de la operación
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        try:
            # Guardar en MongoDB Atlas
            success = self.db.save_operation(operation_id, operation_data)
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
        # Verificar si la operación existe en memoria
        if operation_id in self.async_operations:
            return self.async_operations[operation_id]
        
        # Si no está en memoria, buscar en MongoDB Atlas
        try:
            operation = self.db.get_operation(operation_id)
            
            # Si se encontró, actualizar caché
            if operation:
                self.async_operations[operation_id] = operation
                return operation
            
            return None
        except Exception as e:
            logger.error(f"Error al obtener operación {operation_id} desde MongoDB Atlas: {str(e)}")
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
        
        # Guardar en MongoDB Atlas
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
            try:
                result = self.service_module.process_async_operation(operation_id, a, b)
                # Convertir a diccionario si es necesario
                if hasattr(result, "__dict__"):
                    result_dict = result.__dict__
                elif isinstance(result, dict):
                    result_dict = result
                else:
                    # Si es un objeto de respuesta de gRPC
                    result_dict = {
                        "result": getattr(result, "result", 0),
                        "success": getattr(result, "success", False),
                        "error_message": getattr(result, "error_message", ""),
                        "operation_id": operation_id
                    }
                return result_dict
            except Exception as e:
                logger.error(f"Error al procesar operación con módulo de servicio: {str(e)}")
                # Continuar con la implementación predeterminada
        
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
        operation["timestamp"] = time.time()  # Asegurar timestamp actualizado
        
        # Actualizar resultado si se proporciona
        if result:
            operation["result"] = result
        
        # Guardar cambios en memoria
        self.async_operations[operation_id] = operation
        
        # Guardar en MongoDB Atlas
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
        try:
            # Obtener operaciones pendientes de MongoDB Atlas
            return self.db.get_pending_operations()
        except Exception as e:
            logger.error(f"Error al obtener operaciones pendientes de MongoDB Atlas: {str(e)}")
            
            # Si hay error con MongoDB, usar la caché en memoria
            pending_ops = {}
            for op_id, op_data in self.async_operations.items():
                if op_data.get("status") == OperationStatus.PENDING.value:
                    pending_ops[op_id] = op_data
            
            return pending_ops