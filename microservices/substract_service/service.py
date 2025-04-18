import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)
from common.db.config import OperationsDB

# Inicializar la base de datos
db = OperationsDB.get_instance()

# En lugar de cargar operaciones desde archivos
def load_operations():
    """Ya no se necesita cargar operaciones desde archivos"""
    pass  # La base de datos maneja esto

# En funciones donde guardes operaciones, usa la base de datos
def process_async_operation(operation_id, a, b):
    """
    Procesa una operación asíncrona y actualiza su estado
    """
    # Registrar operación como pendiente
    op_data = {
        "status": operation_pb2.AsyncOperationResponse.OperationStatus.PENDING,
        "message": "Operación en cola",
        "timestamp": time.time(),  # Añadir timestamp
        "service": "sum"  # Identificar el servicio
    }
    
    # Guardar en base de datos
    db.save_operation(operation_id, op_data)
    
    try:
        # Simular procesamiento
        time.sleep(2)
        
        # Actualizar estado a procesando
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.PROCESSING,
            "message": "Procesando operación de suma",
            "timestamp": time.time(),
            "service": "sum"
        }
        db.save_operation(operation_id, op_data)
        
        # Realizar la operación
        result = a + b
        
        # Crear resultado
        result_dict = {
            "result": result,
            "success": True,
            "error_message": "",
            "operation_id": operation_id
        }
        
        # Actualizar estado a completado
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.COMPLETED,
            "message": "Operación de suma completada",
            "result": result_dict,
            "timestamp": time.time(),
            "service": "sum"
        }
        db.save_operation(operation_id, op_data)
        
        return result_dict
    
    except Exception as e:
        # En caso de error
        result_dict = {
            "result": 0,
            "success": False,
            "error_message": str(e),
            "operation_id": operation_id
        }
        
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.FAILED,
            "message": f"Error al procesar suma: {str(e)}",
            "result": result_dict,
            "timestamp": time.time(),
            "service": "sum"
        }
        db.save_operation(operation_id, op_data)
        
        return result_dict