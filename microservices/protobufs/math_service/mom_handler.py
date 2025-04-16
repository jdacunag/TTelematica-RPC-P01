import pika
import json
import uuid
import threading
import time
from service import process_async_operation

# Configuración de RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'
RABBITMQ_VHOST = '/'

# Nombres de las colas y exchanges
OPERATION_QUEUE = 'math_operations'
RESULTS_EXCHANGE = 'operation_results'

class MOMHandler:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.connected = False
        
        # Intentar conectar a RabbitMQ
        self._connect()
        
        # Iniciar thread para procesar mensajes
        if self.connected:
            self.consumer_thread = threading.Thread(target=self._start_consuming)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()
    
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
                credentials=credentials
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
    
    def enqueue_operation(self, a, b, operation_id=None):
        """
        Encola una operación para ser procesada asíncronamente
        """
        if not self.connected:
            self._connect()
            if not self.connected:
                return None, "No se pudo conectar a RabbitMQ"
        
        # Generar ID si no se proporciona
        if not operation_id:
            operation_id = str(uuid.uuid4())
        
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
            return operation_id, "Operación encolada con éxito"
            
        except Exception as e:
            print(f"Error al encolar operación: {str(e)}")
            return None, f"Error al encolar operación: {str(e)}"
    
    def _start_consuming(self):
        """
        Inicia el consumo de mensajes de la cola
        """
        if not self.connected:
            return
        
        try:
            # Configurar consumo de mensajes
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=OPERATION_QUEUE,
                on_message_callback=self._process_message
            )
            
            print("Iniciando consumo de mensajes...")
            self.channel.start_consuming()
            
        except Exception as e:
            print(f"Error al iniciar consumo de mensajes: {str(e)}")
    
    def _process_message(self, ch, method, properties, body):
        """
        Procesa un mensaje recibido de la cola
        """
        try:
            # Decodificar mensaje
            message = json.loads(body)
            print(f"Procesando mensaje: {message}")
            
            # Extraer datos
            operation_id = message.get('operation_id')
            a = message.get('a')
            b = message.get('b')
            
            # Procesar operación
            result = process_async_operation(operation_id, a, b)
            
            # Publicar resultado
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
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def close(self):
        """
        Cierra la conexión con RabbitMQ
        """
        if self.connected:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            
            if self.connection and self.connection.is_open:
                self.connection.close()
            
            self.connected = False
            print("Conexión con RabbitMQ cerrada")


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
    
    # Cerrar conexión
    mom_handler.close()
    
    return operation_id, message