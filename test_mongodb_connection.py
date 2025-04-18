#!/usr/bin/env python3
"""
Script para probar la conexión a MongoDB Atlas
"""
import os
import sys
import json
import uuid
import time

# Añadir directorio raíz al path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.append(project_root)

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

# Imprimir variables para depuración
mongo_uri = os.environ.get('MONGO_URI', 'No encontrado')
mongo_db = os.environ.get('MONGO_DB', 'No encontrado')
mongo_collection = os.environ.get('MONGO_COLLECTION', 'No encontrado')

print("\n=== Información de variables de entorno ===")
# Ocultar la contraseña en la URI para seguridad
if "mongodb+srv://" in mongo_uri:
    parts = mongo_uri.split('@')
    if len(parts) > 1:
        auth_part = parts[0].split('://')
        if len(auth_part) > 1:
            hidden_uri = f"{auth_part[0]}://****:****@{parts[1]}"
            print(f"MONGO_URI: {hidden_uri}")
        else:
            print(f"MONGO_URI: {mongo_uri} (formato incorrecto)")
    else:
        print(f"MONGO_URI: {mongo_uri} (formato incorrecto)")
else:
    print(f"MONGO_URI: {mongo_uri}")
print(f"MONGO_DB: {mongo_db}")
print(f"MONGO_COLLECTION: {mongo_collection}")

print("\n=== Prueba de conexión a MongoDB Atlas ===")
try:
    from common.db.operations_db import OperationsDB
    
    db = OperationsDB.get_instance()
    
    if db.is_connected():
        print("✅ Conexión exitosa a MongoDB Atlas!")
        
        # Intentar contar documentos
        try:
            count = db.count_operations()
            print(f"✅ Número de operaciones almacenadas: {count}")
        except Exception as e:
            print(f"❌ Error al contar operaciones: {e}")
        
        # Intentar guardar una operación de prueba
        try:
            operation_id = f"test-{uuid.uuid4()}"
            operation_data = {
                "status": 3,  # COMPLETED
                "message": "Operación de prueba",
                "result": {
                    "result": 42,
                    "success": True,
                    "error_message": "",
                    "operation_id": operation_id
                },
                "timestamp": time.time(),
                "service": "test",
                "a": 40,
                "b": 2
            }
            
            success = db.save_operation(operation_id, operation_data)
            if success:
                print(f"✅ Operación de prueba guardada correctamente: {operation_id}")
            else:
                print(f"❌ Error al guardar operación de prueba")
        except Exception as e:
            print(f"❌ Error al guardar operación de prueba: {e}")
        
        # Intentar obtener la operación guardada
        try:
            op = db.get_operation(operation_id)
            if op:
                print(f"✅ Operación recuperada correctamente: {op['message']}")
            else:
                print(f"❌ No se pudo recuperar la operación guardada")
        except Exception as e:
            print(f"❌ Error al recuperar operación: {e}")
            
        # Intentar eliminar la operación de prueba
        try:
            success = db.delete_operation(operation_id)
            if success:
                print(f"✅ Operación de prueba eliminada correctamente")
            else:
                print(f"❌ No se pudo eliminar la operación de prueba")
        except Exception as e:
            print(f"❌ Error al eliminar operación: {e}")
    else:
        print("❌ Error: No se pudo conectar a MongoDB Atlas")
except Exception as e:
    print(f"❌ Error al conectar: {e}")

print("\n=== Fin de la prueba ===")