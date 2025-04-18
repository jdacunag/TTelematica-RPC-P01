"""
Módulo centralizado para el manejo de Message-Oriented Middleware (MOM).
Proporciona una interfaz unificada para la comunicación asíncrona entre microservicios.
"""

from .connection import RabbitMQConnection
from .queue_handler import MessageQueueHandler
from .operation_store import OperationStore
from .recovery import OperationRecovery

class MOMHandler:
    """
    Clase principal que gestiona la comunicación mediante MOM con RabbitMQ.
    Coordina las funcionalidades de conexión, cola de mensajes, almacenamiento
    y recuperación de operaciones.
    """
    def __init__(self, service_module=None):
        # Si se proporciona un módulo de servicio, usamos sus funciones
        # Esto permite que el MOMHandler sea independiente de la implementación del servicio
        self.service_module = service_module
        
        # Inicializar componentes
        self.connection = RabbitMQConnection()
        self.store = OperationStore(service_module)
        self.queue_handler = MessageQueueHandler(self.connection, self.store)
        self.recovery = OperationRecovery(self.store, self.queue_handler)
        
        # Iniciar la conexión
        self.connected = self.connection.connect()
        
        # Procesar operaciones pendientes si la conexión es exitosa
        if self.connected:
            self.recovery.process_pending_operations()
            self.queue_handler.start_consumer()
    
    def enqueue_operation(self, a, b, operation_id=None):
        """
        Encola una operación para ser procesada asíncronamente
        
        Args:
            a: Primer operando
            b: Segundo operando
            operation_id: ID opcional de la operación
            
        Returns:
            Tuple (operation_id, message)
        """
        if not self.connection.is_connected():
            self.connection.connect()
            
            if not self.connection.is_connected():
                # Si no podemos conectar, procesamos localmente
                return self.store.process_locally(a, b, operation_id)
            
            # Si la conexión se restableció, iniciar consumidor
            self.queue_handler.start_consumer()
        
        # Encolamos la operación
        return self.queue_handler.publish_operation(a, b, operation_id)
    
    def close(self):
        """
        Cierra la conexión con RabbitMQ
        """
        self.connection.close()

# Función de utilidad para mantener compatibilidad con el código existente
def handle_failover(request, service_module=None):
    """
    Maneja el failover cuando el servicio no está disponible
    
    Args:
        request: Objeto de solicitud con los parámetros de operación
        service_module: Módulo de servicio opcional
        
    Returns:
        Tuple (operation_id, message)
    """
    # Crear instancia del manejador MOM
    mom_handler = MOMHandler(service_module)
    
    # Encolar operación
    operation_id, message = mom_handler.enqueue_operation(
        a=request.a, 
        b=request.b,
        operation_id=request.operation_id if request.operation_id else None
    )
    
    # No cerramos la conexión para que siga procesando mensajes en segundo plano
    return operation_id, message