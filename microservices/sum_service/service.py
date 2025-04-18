import time
import uuid
import json
import os
import operation_pb2
import operation_pb2_grpc
import sys

# Añadir directorio raíz al path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

from common.db.operations_db import OperationsDB

# Inicializar la conexión a MongoDB Atlas
db = OperationsDB.get_instance()

# Ruta para almacenamiento de archivos (mantener para compatibilidad)
OPERATIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "protobufs", "operations")

# Crear directorio si no existe (se mantiene para compatibilidad)
if not os.path.exists(OPERATIONS_DIR):
    os.makedirs(OPERATIONS_DIR)

# Diccionario para cache en memoria de operaciones
async_operations = {}

class SumService(operation_pb2_grpc.SumServiceServicer):
    def __init__(self):
        # Tiempo de inicio del servicio para calcular el uptime
        self.start_time = time.time()
        self.service_id = "sum_service_01"  # ID actualizado
    
    def Sum(self, request, context):
        """
        Implementación de la operación de suma síncrona
        """
        print(f"Recibida solicitud de suma: {request.a} + {request.b}")
        
        # Generar ID de operación si no viene uno
        operation_id = request.operation_id
        if not operation_id:
            operation_id = str(uuid.uuid4())
        
        try:
            # Realizar la operación de suma
            result = request.a + request.b
            
            # Crear la respuesta
            response = operation_pb2.SumResponse(
                result=result,
                success=True,
                error_message="",
                operation_id=operation_id
            )
            
            # Guardar la operación en MongoDB Atlas
            result_dict = {
                "result": result,
                "success": True,
                "error_message": "",
                "operation_id": operation_id
            }
            
            operation_data = {
                "status": operation_pb2.AsyncOperationResponse.OperationStatus.COMPLETED,
                "message": "Operación de suma completada",
                "result": result_dict,
                "timestamp": time.time(),
                "service": "sum",
                "a": request.a,
                "b": request.b,
                "operation_id": operation_id
            }
            
            # Guardar en caché y en MongoDB
            async_operations[operation_id] = operation_data
            success_mongo = db.save_operation(operation_id, operation_data)
            print(f"Guardado en MongoDB Atlas: {'Éxito' if success_mongo else 'Fallo'} - {operation_id}")
            
            # Mantener compatibilidad con archivos
            try:
                file_path = os.path.join(OPERATIONS_DIR, f"{operation_id}.json")
                with open(file_path, 'w') as f:
                    json.dump(operation_data, f, default=lambda o: str(o))
                print(f"Guardado en archivo local: Éxito - {operation_id}")
            except Exception as e:
                print(f"Error al guardar en archivo: {str(e)}")
            
            return response
        except Exception as e:
            # En caso de error
            error_msg = str(e)
            print(f"Error al procesar suma: {error_msg}")
            
            # Crear respuesta de error
            response = operation_pb2.SumResponse(
                result=0,
                success=False,
                error_message=error_msg,
                operation_id=operation_id
            )
            
            # Guardar el error en MongoDB Atlas
            result_dict = {
                "result": 0,
                "success": False,
                "error_message": error_msg,
                "operation_id": operation_id
            }
            
            operation_data = {
                "status": operation_pb2.AsyncOperationResponse.OperationStatus.FAILED,
                "message": f"Error al procesar suma: {error_msg}",
                "result": result_dict,
                "timestamp": time.time(),
                "service": "sum",
                "a": request.a,
                "b": request.b,
                "operation_id": operation_id
            }
            
            # Guardar en caché y en MongoDB
            async_operations[operation_id] = operation_data
            db.save_operation(operation_id, operation_data)
            
            return response
    
    def CheckStatus(self, request, context):
        """
        Implementación para verificar el estado del servicio
        """
        print(f"Verificando estado del servicio {request.service_id}")
        
        # Calcular tiempo de actividad
        uptime = int(time.time() - self.start_time)
        
        # Verificar si el ID de servicio coincide
        if request.service_id == self.service_id:
            return operation_pb2.StatusResponse(
                status=operation_pb2.StatusResponse.ServiceStatus.RUNNING,
                message="Servicio de suma funcionando correctamente",
                uptime=uptime
            )
        else:
            return operation_pb2.StatusResponse(
                status=operation_pb2.StatusResponse.ServiceStatus.UNKNOWN,
                message=f"ID de servicio desconocido: {request.service_id}",
                uptime=uptime
            )
    
    def GetAsyncOperationStatus(self, request, context):
        """
        Implementación para consultar el estado de una operación asíncrona
        """
        operation_id = request.operation_id
        print(f"Consultando estado de operación asíncrona: {operation_id}")
        
        # Buscar operación en caché (memoria)
        if operation_id in async_operations:
            operation = async_operations[operation_id]
        else:
            # Si no está en caché, buscar en MongoDB Atlas
            operation = db.get_operation(operation_id)
            
            # Si se encontró, actualizar caché
            if operation:
                async_operations[operation_id] = operation
        
        # Si se encontró la operación
        if operation:
            # Crear respuesta
            response = operation_pb2.AsyncOperationResponse(
                status=operation["status"],
                message=operation["message"]
            )
            
            # Agregar resultado si está disponible
            if "result" in operation:
                result = operation_pb2.OperationResult()
                result.result = operation["result"]["result"]
                result.success = operation["result"]["success"]
                result.error_message = operation["result"]["error_message"]
                result.operation_id = operation["result"]["operation_id"]
                response.result.CopyFrom(result)
            
            return response
        else:
            # Si no se encontró la operación
            return operation_pb2.AsyncOperationResponse(
                status=operation_pb2.AsyncOperationResponse.OperationStatus.UNKNOWN,
                message=f"Operación no encontrada: {operation_id}"
            )

# Función para procesar operaciones asíncronas (se usará con el MOM)
def process_async_operation(operation_id, a, b):
    """
    Procesa una operación asíncrona y actualiza su estado en MongoDB Atlas
    """
    # Registrar operación como pendiente
    op_data = {
        "status": operation_pb2.AsyncOperationResponse.OperationStatus.PENDING,
        "message": "Operación en cola",
        "timestamp": time.time(),
        "service": "sum",
        "a": a,
        "b": b,
        "operation_id": operation_id
    }
    
    # Guardar en caché y en MongoDB
    async_operations[operation_id] = op_data
    db.save_operation(operation_id, op_data)
    
    try:
        # Actualizar estado a procesando
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.PROCESSING,
            "message": "Procesando operación de suma",
            "timestamp": time.time(),
            "service": "sum",
            "a": a,
            "b": b,
            "operation_id": operation_id
        }
        
        # Guardar en caché y en MongoDB
        async_operations[operation_id] = op_data
        db.save_operation(operation_id, op_data)
        
        # Simular procesamiento
        time.sleep(2)
        
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
            "service": "sum",
            "a": a,
            "b": b,
            "operation_id": operation_id
        }
        
        # Guardar en caché y en MongoDB
        async_operations[operation_id] = op_data
        db.save_operation(operation_id, op_data)
        
        return result_dict
    
    except Exception as e:
        # En caso de error
        error_msg = str(e)
        
        result_dict = {
            "result": 0,
            "success": False,
            "error_message": error_msg,
            "operation_id": operation_id
        }
        
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.FAILED,
            "message": f"Error al procesar suma: {error_msg}",
            "result": result_dict,
            "timestamp": time.time(),
            "service": "sum",
            "a": a,
            "b": b,
            "operation_id": operation_id
        }
        
        # Guardar en caché y en MongoDB
        async_operations[operation_id] = op_data
        db.save_operation(operation_id, op_data)
        
        return result_dict