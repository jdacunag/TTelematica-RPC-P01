#!/usr/bin/env python3
"""
Script para verificar el estado de MongoDB Atlas y RabbitMQ
"""
import sys
import os
import time

# Añadir directorio raíz al path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.append(project_root)

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

def check_mongodb_connection():
    """Verifica la conexión a MongoDB Atlas"""
    try:
        from common.db.operations_db import OperationsDB
        
        print("\n=== Verificación de MongoDB Atlas ===")
        
        # Obtener instancia de OperationsDB
        db = OperationsDB.get_instance()
        
        # Verificar conexión
        if db.is_connected():
            print("✅ Conexión a MongoDB Atlas: ÉXITO")
            
            # Intentar hacer un conteo de operaciones
            try:
                count = db.count_operations()
                print(f"📊 Número de operaciones en MongoDB Atlas: {count}")
            except Exception as e:
                print(f"❌ Error al contar operaciones: {e}")
            
            return True
        else:
            print("❌ Error: No hay conexión a MongoDB Atlas")
            print("Verifica las credenciales en el archivo .env")
            return False
    
    except ImportError:
        print("❌ Error: No se pudo importar OperationsDB")
        print("Asegúrate de que el directorio 'common/db' existe y contiene operations_db.py")
        return False
    except Exception as e:
        print(f"❌ Error general al verificar MongoDB Atlas: {e}")
        return False

def check_rabbitmq_connection():
    """Verifica la conexión a RabbitMQ"""
    try:
        from common.mom.connection import RabbitMQConnection
        
        print("\n=== Verificación de RabbitMQ ===")
        
        # Crear conexión
        connection = RabbitMQConnection()
        
        # Verificar conexión
        if connection.connect():
            print("✅ Conexión a RabbitMQ: ÉXITO")
            
            # Intentar obtener un canal
            channel = connection.get_channel()
            if channel:
                print("✅ Canal de RabbitMQ: ÉXITO")
                
                # Intentar declarar una cola de prueba
                try:
                    result = channel.queue_declare(queue='test_queue', durable=True)
                    print(f"✅ Declaración de cola: ÉXITO (Cola: test_queue)")
                except Exception as e:
                    print(f"❌ Error al declarar cola: {e}")
            else:
                print("❌ Error: No se pudo obtener un canal de RabbitMQ")
            
            # Cerrar conexión
            connection.close()
            return True
        else:
            print("❌ Error: No hay conexión a RabbitMQ")
            print("Verifica que RabbitMQ está ejecutándose y las credenciales son correctas")
            return False
    
    except ImportError:
        print("❌ Error: No se pudo importar RabbitMQConnection")
        print("Asegúrate de que el directorio 'common/mom' existe y contiene connection.py")
        return False
    except Exception as e:
        print(f"❌ Error general al verificar RabbitMQ: {e}")
        return False

def main():
    """Función principal"""
    print("=== Monitor de Servicios ===")
    print("Verificando conexiones a servicios externos...\n")
    
    # Verificar MongoDB Atlas
    mongo_ok = check_mongodb_connection()
    
    # Verificar RabbitMQ
    rabbitmq_ok = check_rabbitmq_connection()
    
    # Mostrar resumen
    print("\n=== Resumen ===")
    print(f"MongoDB Atlas: {'✅ CONECTADO' if mongo_ok else '❌ ERROR'}")
    print(f"RabbitMQ: {'✅ CONECTADO' if rabbitmq_ok else '❌ ERROR'}")
    
    if not mongo_ok or not rabbitmq_ok:
        print("\n⚠️ Hay problemas de conexión. Revisa las credenciales y asegúrate de que los servicios estén activos.")
        sys.exit(1)
    else:
        print("\n✅ Todos los servicios están disponibles y funcionando correctamente.")
        sys.exit(0)

if __name__ == "__main__":
    main()