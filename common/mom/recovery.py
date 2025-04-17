"""
Módulo para gestionar la recuperación de operaciones pendientes.
Implementa la lógica de failover para recuperar operaciones cuando
un servicio vuelve a estar disponible.
"""

import logging
import time

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OperationRecovery:
    """Clase para gestionar la recuperación de operaciones"""
    
    def __init__(self, operation_store, queue_handler=None):
        """
        Inicializa el gestor de recuperación
        
        Args:
            operation_store: Instancia de OperationStore
            queue_handler: Instancia opcional de MessageQueueHandler
        """
        self.operation_store = operation_store
        self.queue_handler = queue_handler
    
    def process_pending_operations(self):
        """
        Procesa todas las operaciones pendientes al iniciar el servidor
        o cuando el servicio se recupera de un fallo
        
        Returns:
            int: Número de operaciones procesadas
        """
        logger.info("Buscando operaciones pendientes...")
        
        # Obtener operaciones pendientes del almacén
        pending_ops = self.operation_store.get_pending_operations()
        
        if not pending_ops:
            logger.info("No hay operaciones pendientes para procesar")
            return 0
        
        logger.info(f"Encontradas {len(pending_ops)} operaciones pendientes")
        
        # Contador de operaciones procesadas
        processed_count = 0
        
        # Procesar cada operación pendiente
        for op_id, op_data in pending_ops.items():
            try:
                # Extraer parámetros de la operación
                a = op_data.get("a")
                b = op_data.get("b")
                
                if a is None or b is None:
                    logger.warning(f"Operación {op_id} incompleta, faltan valores a o b")
                    continue
                
                logger.info(f"Procesando operación pendiente: {op_id} (suma de {a} + {b})")
                
                # Procesar la operación
                self.operation_store.process_operation(op_id, a, b)
                processed_count += 1
                
                # Añadir pequeña pausa para no sobrecargar el sistema
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error al procesar operación pendiente {op_id}: {str(e)}")
        
        logger.info(f"Procesamiento completado. Se procesaron {processed_count} operaciones pendientes.")
        return processed_count
    
    def process_api_gateway_operations(self):
        """
        Busca y procesa operaciones específicamente creadas por el API Gateway
        que tienen un formato ligeramente diferente
        
        Returns:
            int: Número de operaciones procesadas
        """
        logger.info("Buscando operaciones pendientes del API Gateway...")
        
        # Si no hay directorio de operaciones, no hay nada que procesar
        import os
        if not os.path.exists(self.operation_store.operations_dir):
            return 0
        
        processed_count = 0
        
        # Escanear archivos en el directorio
        for filename in os.listdir(self.operation_store.operations_dir):
            if not filename.endswith('.json'):
                continue
            
            op_id = filename[:-5]  # Quitar extensión .json
            
            try:
                file_path = os.path.join(self.operation_store.operations_dir, filename)
                
                # Leer archivo
                with open(file_path, 'r') as f:
                    import json
                    operation_data = json.load(f)
                
                # Verificar si es una operación del API Gateway
                # El API Gateway incluye un mensaje específico
                if (operation_data.get("status") == 1 and  # PENDING
                    "API Gateway" in operation_data.get("message", "")):
                    
                    # Extraer datos
                    a = operation_data.get("a")
                    b = operation_data.get("b")
                    
                    if a is not None and b is not None:
                        logger.info(f"Encontrada operación del API Gateway: {op_id} (suma de {a} + {b})")
                        
                        # Procesar la operación
                        self.operation_store.process_operation(op_id, a, b)
                        processed_count += 1
                        
                        # Añadir pequeña pausa
                        time.sleep(0.1)
                    else:
                        logger.warning(f"Operación {op_id} del API Gateway incompleta, faltan valores")
            
            except Exception as e:
                logger.error(f"Error al procesar archivo {filename}: {str(e)}")
        
        if processed_count > 0:
            logger.info(f"Se procesaron {processed_count} operaciones del API Gateway")
        else:
            logger.info("No se encontraron operaciones pendientes del API Gateway")
        
        return processed_count
    
    def process_files_immediately(self):
        """
        Procesa inmediatamente todos los archivos pendientes
        Esta función puede ser llamada manualmente por el servidor
        
        Returns:
            int: Número total de operaciones procesadas
        """
        logger.info("Procesando archivos pendientes inmediatamente...")
        
        # Procesar operaciones pendientes normales
        count1 = self.process_pending_operations()
        
        # Procesar operaciones del API Gateway
        count2 = self.process_api_gateway_operations()
        
        total = count1 + count2
        logger.info(f"Procesamiento manual completado. Total: {total} operaciones")
        
        return total