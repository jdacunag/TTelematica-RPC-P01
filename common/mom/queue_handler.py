"""
Módulo para gestionar las operaciones con colas de mensajes en RabbitMQ.
Maneja la publicación y consumo de mensajes, así como el procesamiento de los mismos.
"""

import json
import uuid
import threading
import time
import logging
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Nombres de las colas y exchanges
OPERATION_QUEUE = 'math_operations'
RESULTS_EXCHANGE = 'operation_results'

class MessageQueueHandler:
    """Clase para gestionar operaciones de colas de mensajes"""
    
    def __init__(self, connection, operation_store):
        """
        Inicializa el manejador de colas
        
        Args:
            connection: Instancia de RabbitMQConnection
            operation_store: Instancia de OperationStore
        """
        self.connection = connection
        self.operation_store = operation_store
        self.consumer_thread = None
        self._init_queues()
    
    def _init_queues(self):
        """
        Inicializa las colas y exchanges requeridos
        
        Returns:
            bool: True si la inicialización fue exitosa, False en caso contrario
        """
        if not self.connection.is_connected():
            return False
        
        try:
            channel = self.connection.get_channel()
            if not channel:
                logger.error("No se pudo obtener un canal para inicializar colas")
                return False
            
            # Declarar la cola de operaciones (durable para que sobreviva a reinicios)
            channel.queue_declare(queue=OPERATION_QUEUE, durable=True)
            
            # Declarar el exchange para resultados
            channel.exchange_declare(
                exchange=RESULTS_EXCHANGE,
                exchange_type='topic',
                durable=True
            )
            
            logger.info("Colas y exchanges inicializados correctamente")
            return True
        except Exception as e:
            logger.error(f"Error al inicializar colas: {e}")
            return False
    
    def start_consumer(self):
        """
        Inicia el thread para consumir mensajes si no está activo
        
        Returns:
            bool: True si el consumidor se inició o ya estaba activo, False en caso contrario
        """
        if self.consumer_thread is None or not self.consumer_thread.is_alive():
            self.consumer_thread = threading.Thread(target=self._start_consuming)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()
            logger.info("Thread de consumo iniciado")
            return True
        return True
    
    def _start_consuming(self):
        """Inicia el consumo de mensajes de la cola"""
        if not self.connection.is_connected():
            logger.error("No se puede iniciar consumo, sin conexión")
            return
        
        try:
            channel = self.connection.get_channel()
            if not channel:
                logger.error("No se pudo obtener un canal para consumir mensajes")
                return
            
            # Configurar consumo de mensajes
            channel.basic_qos(prefetch_count=1)
            
            # Crear nuevos parámetros para el consumo
            channel.basic_consume(
                queue=OPERATION_QUEUE,
                on_message_callback=self._process_message_callback,
                auto_ack=False  # Importante: que no confirme automáticamente
            )
            
            logger.info("Iniciando consumo de mensajes...")
            channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Error al iniciar consumo de mensajes: {str(e)}")
            # Esperar un momento antes de intentar reconectar
            time.sleep(5)
            self.connection.reconnect()
    
    def _process_message_callback(self, ch, method, properties, body):
        """
        Callback para procesar mensajes recibidos
        
        Args:
            ch: Canal
            method: Método de entrega
            properties: Propiedades del mensaje
            body: Contenido del mensaje
        """
        operation_id = None
        try:
            # Decodificar mensaje
            message = json.loads(body)
            logger.info(f"Procesando mensaje: {message}")
            
            # Extraer datos
            operation_id = message.get('operation_id')
            a = message.get('a')
            b = message.get('b')
            
            if not operation_id or a is None or b is None:
                logger.warning("Mensaje inválido, falta información")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            # Procesar operación usando el store
            result = self.operation_store.process_operation(operation_id, a, b)
            
            # IMPORTANTE: Asegurar que la operación se guarde en el directorio de operaciones
            # para que aparezca en el listado de API Gateway
            self._save_operation_for_api_gateway(operation_id, result, a, b)
            
            # Publicar resultado en RabbitMQ también
            result_message = {
                'operation_id': operation_id,
                'result': result.get('result', 0) if isinstance(result, dict) else result,
                'success': result.get('success', False) if isinstance(result, dict) else False,
                'error_message': result.get('error_message', '') if isinstance(result, dict) else ''
            }
            
            # Publicar resultado en el exchange de resultados
            self._publish_result(operation_id, result_message)
            
            # Confirmar procesamiento del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Mensaje {operation_id} procesado correctamente")
            
        except Exception as e:
            logger.error(f"Error al procesar mensaje: {str(e)}")
            
            # Rechazar mensaje para que vuelva a la cola
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Mensaje rechazado y devuelto a la cola")
            except Exception:
                logger.error("No se pudo rechazar el mensaje")
                
            # Si el error es grave, intentar registrar la operación como fallida
            if operation_id:
                try:
                    self.operation_store.mark_as_failed(operation_id, str(e))
                except Exception:
                    logger.error("No se pudo actualizar el estado de la operación")
    
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
            # Esta ruta debe coincidir con la que usa el API Gateway
            api_gateway_operations_dir = None
            
            # Primero, intentar obtener la ruta desde el módulo de servicio
            if hasattr(self.operation_store, "service_module") and self.operation_store.service_module:
                if hasattr(self.operation_store.service_module, "OPERATIONS_DIR"):
                    api_gateway_operations_dir = self.operation_store.service_module.OPERATIONS_DIR
            
            # Si no se pudo obtener la ruta, usar la ruta predeterminada
            if not api_gateway_operations_dir:
                # Intentar encontrar el directorio más probable
                # Asumimos que estamos en un subdirectorio de la carpeta principal del proyecto
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
            
            # Determinar el estado correcto
            status = 3  # COMPLETED
            if isinstance(result, dict) and not result.get('success', False):
                status = 4  # FAILED
            
            # Crear datos de la operación en el formato que espera el API Gateway
            operation_data = {
                "status": status,
                "message": "Operación completada" if status == 3 else "Operación fallida",
                "timestamp": time.time(),
                "result": {
                    "result": result.get('result', 0) if isinstance(result, dict) else result,
                    "success": result.get('success', True) if isinstance(result, dict) else True,
                    "error_message": result.get('error_message', "") if isinstance(result, dict) else "",
                    "operation_id": operation_id
                },
                "a": a,
                "b": b
            }
            
            # Guardar en archivo
            file_path = os.path.join(api_gateway_operations_dir, f"{operation_id}.json")
            with open(file_path, 'w') as f:
                json.dump(operation_data, f)
            
            logger.info(f"Operación {operation_id} guardada para API Gateway en {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error al guardar operación para API Gateway: {str(e)}")
            return False
    
    def _publish_result(self, operation_id, result_message):
        """
        Publica el resultado de una operación en el exchange de resultados
        
        Args:
            operation_id: ID de la operación
            result_message: Mensaje con el resultado
            
        Returns:
            bool: True si se publicó correctamente, False en caso contrario
        """
        try:
            channel = self.connection.get_channel()
            if not channel:
                logger.error("No se pudo obtener un canal para publicar resultados")
                return False
            
            # Asegurar que existe el exchange
            channel.exchange_declare(
                exchange=RESULTS_EXCHANGE,
                exchange_type='topic',
                durable=True
            )
            
            # Publicar el mensaje
            channel.basic_publish(
                exchange=RESULTS_EXCHANGE,
                routing_key=f"result.{operation_id}",
                body=json.dumps(result_message),
                properties=channel.basic_properties(
                    delivery_mode=2,  # Mensaje persistente
                    content_type='application/json'
                )
            )
            
            logger.info(f"Resultado para operación {operation_id} publicado")
            return True
        except Exception as e:
            logger.error(f"Error al publicar resultado: {str(e)}")
            return False
    
    def publish_operation(self, a, b, operation_id=None):
        """
        Publica una operación en la cola de mensajes
        
        Args:
            a: Primer operando
            b: Segundo operando
            operation_id: ID opcional de la operación
            
        Returns:
            Tuple (operation_id, message): ID de la operación y mensaje de estado
        """
        if not self.connection.is_connected():
            success = self.connection.reconnect()
            if not success:
                logger.error("No se pudo conectar para encolar operación")
                return None, "Error: No hay conexión disponible"
        
        # Generar ID si no se proporciona
        if not operation_id:
            operation_id = str(uuid.uuid4())
        
        # Registrar operación como pendiente usando el store
        self.operation_store.register_pending_operation(operation_id, a, b)
        
        # IMPORTANTE: También registrar la operación pendiente para el API Gateway
        try:
            # Determinar la ruta del directorio de operaciones para el API Gateway
            api_gateway_operations_dir = None
            
            # Primero, intentar obtener la ruta desde el módulo de servicio
            if hasattr(self.operation_store, "service_module") and self.operation_store.service_module:
                if hasattr(self.operation_store.service_module, "OPERATIONS_DIR"):
                    api_gateway_operations_dir = self.operation_store.service_module.OPERATIONS_DIR
            
            # Si no se pudo obtener la ruta, usar una ruta predeterminada
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
            
            # Crear datos de la operación pendiente
            operation_data = {
                "status": 1,  # PENDING
                "message": "Operación en cola (MOM)",
                "timestamp": time.time(),
                "a": a,
                "b": b
            }
            
            # Guardar en archivo
            file_path = os.path.join(api_gateway_operations_dir, f"{operation_id}.json")
            with open(file_path, 'w') as f:
                json.dump(operation_data, f)
            
            logger.info(f"Operación pendiente {operation_id} guardada para API Gateway en {file_path}")
        except Exception as e:
            logger.error(f"Error al guardar operación pendiente para API Gateway: {str(e)}")
        
        # Crear mensaje con los datos de la operación
        message = {
            'operation': 'sum',
            'a': a,
            'b': b,
            'operation_id': operation_id,
            'timestamp': time.time()
        }
        
        try:
            # Obtener canal válido
            channel = self.connection.get_channel()
            if not channel:
                logger.error("No se pudo obtener un canal para publicar la operación")
                return operation_id, "Error: No se pudo publicar, pero la operación está registrada"
            
            # Asegurar que existe la cola
            channel.queue_declare(queue=OPERATION_QUEUE, durable=True)
            
            # Publicar mensaje en la cola
            channel.basic_publish(
                exchange='',
                routing_key=OPERATION_QUEUE,
                body=json.dumps(message),
                properties=channel.basic_properties(
                    delivery_mode=2,  # Mensaje persistente
                    content_type='application/json'
                )
            )
            
            logger.info(f"Operación {operation_id} encolada correctamente")
            
            # Importante: intentar reconectar el consumidor si no está activo
            if self.consumer_thread is None or not self.consumer_thread.is_alive():
                logger.info("Reiniciando thread de consumo")
                self.start_consumer()
                
            return operation_id, "Operación encolada con éxito"
            
        except Exception as e:
            logger.error(f"Error al encolar operación: {str(e)}")
            return operation_id, f"Error al encolar operación: {str(e)}"