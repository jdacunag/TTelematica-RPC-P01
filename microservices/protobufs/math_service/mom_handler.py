import pika
import json
import uuid
import threading
import time
from service import process_async_operation, save_operation, async_operations

# Configuración de RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'
RABBITMQ_VHOST = '/'

# Nombres de las colas y exchanges
OPERATION_QUEUE = 'math_operations'
RESULTS_EXCHANGE = 'operation_results'

def process_pending_operations():
    """
    Procesa todas las operaciones pendientes al iniciar el servidor
    """
    print("Buscando operaciones pendientes...")
    
    # Buscar operaciones en PENDING
    pending_ops = {}
    for op_id, op_data in async_operations.items():
        if op_data.get("status") == 1:  # PENDING
            pending_ops[op_id] = op_data
    
    if not pending_ops:
        print("No hay operaciones pendientes")
        return
    
    print(f"Encontradas {len(pending_ops)} operaciones pendientes")
    
    # Procesar cada operación pendiente
    for op_id, op_data in pending_ops.items():
        try:
            # Extraer a y b
            a = op_data.get("a")
            b = op_data.get("b")
            
            if a is None or b is None:
                print(f"Operación {op_id} incompleta, falta a o b")
                continue
            
            print(f"Procesando operación pendiente: {op_id} (suma de {a} + {b})")
            
            # Procesar la operación
            process_async_operation(op_id, a, b)
            
        except Exception as e:
            print(f"Error al procesar operación pendiente {op_id}: {str(e)}")

class MOMHandler:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.connected = False
        self.consumer_thread = None
        
        # Intentar conectar a RabbitMQ
        self._connect()
        
        # Procesar operaciones pendientes
        process_pending_operations()
        
        # Iniciar thread para procesar mensajes
        if self.connected:
            self._start_consumer_thread()
    
    def _connect(self):
        """Establece conexión con RabbitMQ"""
        try:
            # Credenciales para la conexión
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            
            # Parámetros de conexión
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                heartbeat=600,  # Incrementar heartbeat para evitar desconexiones
                blocked_connection_timeout=300
            )
            
            # Crear conexión y canal
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            # Declarar la cola de operaciones (durable para que sobreviva a reinicios)
            self.channel.queue_declare(queue=OPERATION_QUEUE, durable=True)
            
            # Declarar el exchange para resultados
            self.channel.exchange_declare(
                exchange=RESULTS_EXCHANGE,
                exchange_type='topic',
                durable=True
            )
            
            print("Conexión establecida con RabbitMQ")
            self.connected = True
            
        except Exception as e:
            print(f"Error al conectar con RabbitMQ: {str(e)}")
            self.connected = False
    
    def _start_consumer_thread(self):
        """Inicia el thread para consumir mensajes"""
        if self.consumer_thread is None or not self.consumer_thread.is_alive():
            self.consumer_thread = threading.Thread(target=self._start_consuming)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()
            print("Thread de consumo iniciado")
    
    def enqueue_operation(self, a, b, operation_id=None):
        """
        Encola una operación para ser procesada asíncronamente
        """
        if not self.connected:
            self._connect()
            if not self.connected:
                return None, "No se pudo conectar a RabbitMQ"
            
            # Si la conexión se restableció, iniciar consumidor
            self._start_consumer_thread()
        
        # Generar ID si no se proporciona
        if not operation_id:
            operation_id = str(uuid.uuid4())
        
        # Registrar operación como pendiente en memoria y disco
        op_data = {
            "status": 1,  # PENDING (equivale a OperationStatus.PENDING)
            "message": "Operación en cola",
            "a": a,
            "b": b,
            "timestamp": time.time()
        }
        async_operations[operation_id] = op_data
        save_operation(operation_id, op_data)
        
        # Crear mensaje con los datos de la operación
        message = {
            'operation': 'sum',
            'a': a,
            'b': b,
            'operation_id': operation_id,
            'timestamp': time.time()
        }
        
        try:
            # Publicar mensaje en la cola
            self.channel.basic_publish(
                exchange='',
                routing_key=OPERATION_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensaje persistente
                    content_type='application/json'
                )
            )
            
            print(f"Operación {operation_id} encolada correctamente")
            
            # Importante: intentar reconectar el consumidor si no está activo
            if self.consumer_thread is None or not self.consumer_thread.is_alive():
                print("Reiniciando thread de consumo")
                self._start_consumer_thread()
                
            return operation_id, "Operación encolada con éxito"
            
        except Exception as e:
            print(f"Error al encolar operación: {str(e)}")
            return None, f"Error al encolar operación: {str(e)}"
    
    def _start_consuming(self):
        """
        Inicia el consumo de mensajes de la cola
        """
        if not self.connected:
            print("No se puede iniciar consumo, sin conexión")
            return
        
        try:
            # Verificar si el canal está abierto, sino reconectar
            if not self.channel or not self.channel.is_open:
                print("Canal cerrado, reconectando...")
                self._connect()
                if not self.connected:
                    print("No se pudo reconectar")
                    return
            
            # Configurar consumo de mensajes
            self.channel.basic_qos(prefetch_count=1)
            
            # Crear nuevos parámetros para el consumo
            self.channel.basic_consume(
                queue=OPERATION_QUEUE,
                on_message_callback=self._process_message,
                auto_ack=False  # Importante: que no confirme automáticamente
            )
            
            print("Iniciando consumo de mensajes...")
            self.channel.start_consuming()
            
        except Exception as e:
            print(f"Error al iniciar consumo de mensajes: {str(e)}")
            # Esperar un momento antes de intentar reconectar
            time.sleep(5)
            self._connect()
    
    def _process_message(self, ch, method, properties, body):
        """
        Procesa un mensaje recibido de la cola
        """
        operation_id = None
        try:
            # Decodificar mensaje
            message = json.loads(body)
            print(f"Procesando mensaje: {message}")
            
            # Extraer datos
            operation_id = message.get('operation_id')
            a = message.get('a')
            b = message.get('b')
            
            if not operation_id or a is None or b is None:
                print("Mensaje inválido, falta información")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            # Procesar operación (esto incluye guardar en disco)
            result = process_async_operation(operation_id, a, b)
            
            # Publicar resultado en RabbitMQ también
            result_message = {
                'operation_id': operation_id,
                'result': result.result,
                'success': result.success,
                'error_message': result.error_message
            }
            
            self.channel.basic_publish(
                exchange=RESULTS_EXCHANGE,
                routing_key=f"result.{operation_id}",
                body=json.dumps(result_message),
                properties=pika.BasicProperties(
                    content_type='application/json'
                )
            )
            
            # Confirmar procesamiento del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"Mensaje {operation_id} procesado correctamente")
            
        except Exception as e:
            print(f"Error al procesar mensaje: {str(e)}")
            
            # Rechazar mensaje para que vuelva a la cola
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                print("No se pudo rechazar el mensaje")
                
            # Si el error es grave, intentar registrar la operación como fallida
            if operation_id:
                try:
                    op_data = {
                        "status": 4,  # FAILED
                        "message": f"Error al procesar: {str(e)}"
                    }
                    async_operations[operation_id] = op_data
                    save_operation(operation_id, op_data)
                except Exception:
                    print("No se pudo actualizar el estado de la operación")
    
    def close(self):
        """
        Cierra la conexión con RabbitMQ
        """
        if self.connected:
            try:
                if self.channel and self.channel.is_open:
                    self.channel.stop_consuming()
                
                if self.connection and self.connection.is_open:
                    self.connection.close()
                
                self.connected = False
                print("Conexión con RabbitMQ cerrada")
            except Exception as e:
                print(f"Error al cerrar conexión: {str(e)}")


# Función para manejar caso de failover en el servidor
def handle_failover(request):
    """
    Maneja el failover cuando el servicio no está disponible
    """
    # Crear instancia del manejador MOM
    mom_handler = MOMHandler()
    
    # Encolar operación
    operation_id, message = mom_handler.enqueue_operation(
        a=request.a, 
        b=request.b,
        operation_id=request.operation_id if request.operation_id else None
    )
    
    # No cerramos la conexión para que siga procesando mensajes en segundo plano
    # mom_handler.close()
    
    return operation_id, message