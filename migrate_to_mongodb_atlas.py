#!/usr/bin/env python3
"""
Script para migrar operaciones de archivos JSON a MongoDB Atlas
"""
import os
import json
import sys
import glob
import time

# Añadir directorio raíz al path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.append(project_root)

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

# Importar la clase OperationsDB
try:
    from common.db.operations_db import OperationsDB
except ImportError:
    print("Error: No se pudo importar OperationsDB")
    print("Asegúrate de que el directorio 'common/db' existe y contiene operations_db.py")
    sys.exit(1)

def migrate_files_to_mongodb_atlas():
    """Migra todos los archivos JSON de operaciones a MongoDB Atlas"""
    print("Iniciando migración a MongoDB Atlas...")
    
    # Obtener instancia de OperationsDB
    db = OperationsDB.get_instance()
    
    # Verificar conexión
    if not db.is_connected():
        print("\nError: No se pudo conectar a MongoDB Atlas")
        print("Verifique que las variables de entorno estén configuradas correctamente en el archivo .env:")
        print("  MONGO_URI=mongodb+srv://usuario:password@cluster.mongodb.net/microservices_db?retryWrites=true&w=majority")
        print("  MONGO_DB=microservices_db")
        print("  MONGO_COLLECTION=operations")
        sys.exit(1)
    
    # Directorio donde se almacenan las operaciones
    operations_dir = os.path.join(project_root, "microservices", "protobufs", "operations")
    
    # Verificar si el directorio existe
    if not os.path.exists(operations_dir):
        print(f"El directorio de operaciones no existe: {operations_dir}")
        print("Creando directorio...")
        try:
            os.makedirs(operations_dir)
        except Exception as e:
            print(f"Error al crear directorio: {e}")
            sys.exit(1)
    
    # Buscar todos los archivos JSON
    json_files = glob.glob(os.path.join(operations_dir, "*.json"))
    
    print(f"\nEncontrados {len(json_files)} archivos JSON para migrar")
    
    success_count = 0
    error_count = 0
    skip_count = 0
    
    for file_path in json_files:
        try:
            # Extraer ID de operación del nombre del archivo
            filename = os.path.basename(file_path)
            operation_id = filename[:-5]  # Quitar extensión .json
            
            # Verificar si la operación ya existe en MongoDB Atlas
            existing = db.get_operation(operation_id)
            if existing:
                print(f"Omitiendo {operation_id}: ya existe en MongoDB Atlas")
                skip_count += 1
                continue
            
            # Leer archivo
            try:
                with open(file_path, 'r') as f:
                    operation_data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON en {filename}: {e}")
                error_count += 1
                continue
            
            # Asegurar que el operation_id esté incluido
            operation_data['operation_id'] = operation_id
            
            # Si no tiene campo service, añadir uno por defecto
            if 'service' not in operation_data:
                # Intentar detectar servicio por datos
                if 'result' in operation_data and isinstance(operation_data['result'], dict):
                    if 'a' in operation_data and 'b' in operation_data:
                        if operation_data['result'].get('result', 0) == operation_data.get('a', 0) + operation_data.get('b', 0):
                            operation_data['service'] = 'sum'
                        elif operation_data['result'].get('result', 0) == operation_data.get('a', 0) - operation_data.get('b', 0):
                            operation_data['service'] = 'subtract'
                        else:
                            operation_data['service'] = 'unknown'
                    else:
                        operation_data['service'] = 'unknown'
                else:
                    operation_data['service'] = 'unknown'
            
            # Asegurar que hay un timestamp
            if 'timestamp' not in operation_data:
                operation_data['timestamp'] = time.time()
            
            # Guardar en MongoDB Atlas
            success = db.save_operation(operation_id, operation_data)
            
            if success:
                success_count += 1
                print(f"Migrado: {operation_id}")
            else:
                error_count += 1
                print(f"Error al migrar: {operation_id}")
                
        except Exception as e:
            error_count += 1
            print(f"Error procesando {file_path}: {str(e)}")
    
    print(f"\nMigración completada:")
    print(f"- {success_count} operaciones migradas exitosamente")
    print(f"- {skip_count} operaciones omitidas (ya existentes)")
    print(f"- {error_count} errores")

if __name__ == "__main__":
    migrate_files_to_mongodb_atlas()