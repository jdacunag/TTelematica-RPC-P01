"""
Módulo para gestionar la recuperación de operaciones pendientes.
Implementa la lógica de failover para recuperar operaciones cuando
un servicio vuelve a estar disponible.
"""

import logging
import time
import os
import json

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
                result = self.operation_store.process_operation(op_id, a, b)
                
                # IMPORTANTE: Guardar explícitamente la operación en el formato que espera el API Gateway
                self._save_operation_for_api_gateway(op_id, result, a, b)
                
                processed_count += 1
                
                # Añadir pequeña pausa para no sobrecargar el sistema
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error al procesar operación pendiente {op_id}: {str(e)}")
        
        logger.info(f"Procesamiento completado. Se procesaron {processed_count} operaciones pendientes.")
        return processed_count
    
    def _save_operation_for_api_gateway(self, operation_id, result, a, b):
        """
        Guarda la operación en el formato que espera el API Gateway
        
        Args:
            operation_id: ID de la operación
            result: Resultado de la operación
            a: Primer operando
            b: Segundo operando
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        try:
            # Determinar la ruta del directorio de operaciones para el API Gateway
            api_gateway_operations_dir = None
            
            # Primero, intentar obtener la ruta desde el módulo de servicio
            if hasattr(self.operation_store, "service_module") and self.operation_store.service_module:
                if hasattr(self.operation_store.service_module, "OPERATIONS_DIR"):
                    api_gateway_operations_dir = self.operation_store.service_module.OPERATIONS_DIR
            
            if not api_gateway_operations_dir:
                # Intentar encontrar el directorio más probable
                current_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
                possible_paths = [
                    os.path.join(project_root, "microservices", "protobufs", "math_service", "operations"),
                    os.path.join(os.getcwd(), "operations"),
                    os.path.join(os.getcwd(), "microservices", "protobufs", "math_service", "operations")
                ]
                
                for path in possible_paths:
                    if os.path.exists(path) or os.access(os.path.dirname(path), os.W_OK):
                        api_gateway_operations_dir = path
                        break
                
                if not api_gateway_operations_dir:
                    # Si no se encontró ninguna ruta válida, usar la ruta del operation_store
                    api_gateway_operations_dir = self.operation_store.operations_dir
            
            # Crear directorio si no existe
            if not os.path.exists(api_gateway_operations_dir):
                os.makedirs(api_gateway_operations_dir)
            
            # Convertir el resultado a un formato serializable si es necesario
            result_value = 0
            success = False
            error_message = ""
            
            # Manejar diferentes formatos de resultado
            if hasattr(result, 'result'):
                # Si es un objeto con atributo result
                result_value = result.result
                success = True if hasattr(result, 'success') and result.success else False
                error_message = result.error_message if hasattr(result, 'error_message') else ""
            elif isinstance(result, dict):
                # Si es un diccionario
                result_value = result.get('result', 0)
                success = result.get('success', False)
                error_message = result.get('error_message', "")
            elif isinstance(result, (int, float)):
                # Si es un valor numérico directo
                result_value = result
                success = True
            
            # Determinar el estado correcto
            status = 3 if success else 4  # 3=COMPLETED, 4=FAILED
            
            # Crear datos de la operación en el formato que espera el API Gateway
            operation_data = {
                "status": status,
                "message": "Operación completada" if status == 3 else "Operación fallida",
                "timestamp": time.time(),
                "result": {
                    "result": result_value,
                    "success": success,
                    "error_message": error_message,
                    "operation_id": operation_id
                },
                "a": a,
                "b": b
            }
            
            # Guardar en archivo con manejo seguro para JSON
            file_path = os.path.join(api_gateway_operations_dir, f"{operation_id}.json")
            with open(file_path, 'w') as f:
                json.dump(operation_data, f, default=lambda o: str(o))
            
            logger.info(f"Operación {operation_id} guardada para API Gateway en {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error al guardar operación para API Gateway: {str(e)}")
            return False
    
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
                
                # Leer archivo con manejo de errores
                try:
                    with open(file_path, 'r') as f:
                        import json
                        operation_data = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Error al decodificar JSON en archivo {filename}: {str(e)}")
                    continue
                
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
                        result = self.operation_store.process_operation(op_id, a, b)
                        
                        # Guardar explícitamente en el formato del API Gateway
                        self._save_operation_for_api_gateway(op_id, result, a, b)
                        
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