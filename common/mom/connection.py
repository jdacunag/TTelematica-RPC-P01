"""
Módulo para gestionar las conexiones con RabbitMQ.
Proporciona funcionalidades para establecer, mantener y cerrar conexiones.
"""

import pika
import time
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de RabbitMQ (podría moverse a un archivo de configuración)
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'
RABBITMQ_VHOST = '/'

class RabbitMQConnection:
    """Clase para gestionar la conexión con RabbitMQ"""
    
    def __init__(self):
        """Inicializa el estado de la conexión"""
        self.connection = None
        self.channel = None
        self._connected = False
    
    def connect(self):
        """
        Establece conexión con RabbitMQ.
        Intenta primero con las credenciales configuradas y, si falla,
        prueba con las credenciales por defecto.
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario
        """
        try:
            # Usar credenciales configuradas
            logger.info(f"Intentando conexión a RabbitMQ con credenciales configuradas: {RABBITMQ_USER}...")
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            
            # Parámetros de conexión
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            # Crear conexión y canal
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            logger.info("Conexión establecida con RabbitMQ")
            self._connected = True
            
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión AMQP: {e}")
            logger.info("Intentando con credenciales alternativas (guest/guest)...")
            
            try:
                # Intentar con credenciales por defecto de RabbitMQ
                credentials = pika.PlainCredentials('guest', 'guest')
                params = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    virtual_host=RABBITMQ_VHOST,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                
                logger.info("Conexión establecida con RabbitMQ usando credenciales por defecto")
                self._connected = True
            except Exception as e2:
                logger.error(f"Error al conectar con credenciales alternativas: {e2}")
                self._connected = False
        except Exception as e:
            logger.error(f"Error al conectar con RabbitMQ: {e}")
            self._connected = False
        
        return self._connected
    
    def is_connected(self):
        """
        Verifica si la conexión está activa
        
        Returns:
            bool: True si está conectado, False en caso contrario
        """
        return self._connected and self.connection is not None and self.connection.is_open
    
    def reconnect(self):
        """
        Intenta reconectar si se ha perdido la conexión
        
        Returns:
            bool: True si la reconexión fue exitosa, False en caso contrario
        """
        if not self.is_connected():
            logger.info("Reconectando con RabbitMQ...")
            return self.connect()
        return True
    
    def close(self):
        """Cierra la conexión con RabbitMQ"""
        if self._connected:
            try:
                if self.channel and self.channel.is_open:
                    self.channel.close()
                
                if self.connection and self.connection.is_open:
                    self.connection.close()
                
                self._connected = False
                logger.info("Conexión con RabbitMQ cerrada")
                return True
            except Exception as e:
                logger.error(f"Error al cerrar conexión: {e}")
                return False
        return True
    
    def get_channel(self):
        """
        Obtiene el canal actual, reconectando si es necesario
        
        Returns:
            pika.Channel: Canal activo de RabbitMQ o None si no se pudo obtener
        """
        if not self.is_connected() or not self.channel or not self.channel.is_open:
            success = self.reconnect()
            if not success:
                return None
        return self.channel