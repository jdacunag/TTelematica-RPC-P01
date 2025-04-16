import time
import uuid
import json
import os
import operation_pb2
import operation_pb2_grpc

# Ruta para almacenar operaciones de forma persistente
OPERATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "operations")

# Crear directorio si no existe
if not os.path.exists(OPERATIONS_DIR):
    os.makedirs(OPERATIONS_DIR)

# Diccionario para almacenar operaciones asíncronas en memoria
async_operations = {}

# Cargar operaciones existentes desde almacenamiento
def load_operations():
    """Carga operaciones desde archivos al diccionario en memoria"""
    if not os.path.exists(OPERATIONS_DIR):
        return
    
    for filename in os.listdir(OPERATIONS_DIR):
        if filename.endswith('.json'):
            try:
                operation_id = filename[:-5]  # Quitar extensión .json
                file_path = os.path.join(OPERATIONS_DIR, filename)
                
                with open(file_path, 'r') as f:
                    operation_data = json.load(f)
                    async_operations[operation_id] = operation_data
                    print(f"Cargada operación: {operation_id}")
            except Exception as e:
                print(f"Error al cargar operación {filename}: {str(e)}")

# Guardar operación en almacenamiento persistente
def save_operation(operation_id, operation_data):
    """Guarda una operación en un archivo JSON"""
    try:
        # Asegurarnos de que todos los campos necesarios están presentes
        if "a" not in operation_data and "b" not in operation_data:
            # Si estos datos están en async_operations, usarlos
            if operation_id in async_operations:
                existing_data = async_operations[operation_id]
                if "a" in existing_data:
                    operation_data["a"] = existing_data["a"]
                if "b" in existing_data:
                    operation_data["b"] = existing_data["b"]
        
        file_path = os.path.join(OPERATIONS_DIR, f"{operation_id}.json")
        with open(file_path, 'w') as f:
            json.dump(operation_data, f)
    except Exception as e:
        print(f"Error al guardar operación {operation_id}: {str(e)}")

# Cargar operaciones al inicio
load_operations()

class MathService(operation_pb2_grpc.MathServiceServicer):
    def __init__(self):
        # Tiempo de inicio del servicio para calcular el uptime
        self.start_time = time.time()
        self.service_id = "math_service_01"
    
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
            
            # Crear y retornar la respuesta
            return operation_pb2.OperationResponse(
                result=result,
                success=True,
                error_message="",
                operation_id=operation_id
            )
        except Exception as e:
            # En caso de error, retornar respuesta con error
            return operation_pb2.OperationResponse(
                result=0,
                success=False,
                error_message=str(e),
                operation_id=operation_id
            )
    
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
                message="Servicio funcionando correctamente",
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
        
        # Verificar si la operación existe en memoria
        if operation_id in async_operations:
            operation = async_operations[operation_id]
            
            # Crear respuesta
            response = operation_pb2.AsyncOperationResponse(
                status=operation["status"],
                message=operation["message"]
            )
            
            # Agregar resultado si está disponible
            if "result" in operation:
                response.result.CopyFrom(operation["result"])
            
            return response
        else:
            # Si no está en memoria, buscar en almacenamiento persistente
            file_path = os.path.join(OPERATIONS_DIR, f"{operation_id}.json")
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        operation = json.load(f)
                        
                    # Cargar en memoria
                    async_operations[operation_id] = operation
                    
                    # Crear respuesta
                    response = operation_pb2.AsyncOperationResponse(
                        status=operation["status"],
                        message=operation["message"]
                    )
                    
                    # Agregar resultado si está disponible
                    if "result" in operation:
                        result = operation_pb2.OperationResponse()
                        result.result = operation["result"]["result"]
                        result.success = operation["result"]["success"] 
                        result.error_message = operation["result"].get("error_message", "")
                        result.operation_id = operation["result"].get("operation_id", operation_id)
                        response.result.CopyFrom(result)
                    
                    return response
                except Exception as e:
                    print(f"Error al cargar operación desde archivo: {str(e)}")
            
            # Si no existe, retornar estado desconocido
            return operation_pb2.AsyncOperationResponse(
                status=operation_pb2.AsyncOperationResponse.OperationStatus.UNKNOWN,
                message=f"Operación no encontrada: {operation_id}"
            )

# Función para procesar operaciones asíncronas (se usará con el MOM)
def process_async_operation(operation_id, a, b):
    """
    Procesa una operación asíncrona y actualiza su estado
    """
    # Registrar operación como pendiente
    op_data = {
        "status": operation_pb2.AsyncOperationResponse.OperationStatus.PENDING,
        "message": "Operación en cola",
        "a": a,
        "b": b
    }
    async_operations[operation_id] = op_data
    save_operation(operation_id, op_data)
    
    try:
        # Simular procesamiento
        time.sleep(2)
        
        # Actualizar estado a procesando
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.PROCESSING,
            "message": "Procesando operación",
            "a": a,
            "b": b
        }
        async_operations[operation_id] = op_data
        save_operation(operation_id, op_data)
        
        # Realizar la operación
        result = a + b
        
        # Crear resultado y actualizar estado a completado
        response = operation_pb2.OperationResponse(
            result=result,
            success=True,
            error_message="",
            operation_id=operation_id
        )
        
        # Convertir a diccionario para almacenamiento
        result_dict = {
            "result": result,
            "success": True,
            "error_message": "",
            "operation_id": operation_id
        }
        
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.COMPLETED,
            "message": "Operación completada",
            "result": result_dict,
            "a": a,
            "b": b
        }
        async_operations[operation_id] = op_data
        save_operation(operation_id, op_data)
        
        return response
    
    except Exception as e:
        # En caso de error, actualizar estado a fallido
        response = operation_pb2.OperationResponse(
            result=0,
            success=False,
            error_message=str(e),
            operation_id=operation_id
        )
        
        # Convertir a diccionario para almacenamiento
        result_dict = {
            "result": 0,
            "success": False,
            "error_message": str(e),
            "operation_id": operation_id
        }
        
        op_data = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.FAILED,
            "message": f"Error al procesar: {str(e)}",
            "result": result_dict,
            "a": a,
            "b": b
        }
        async_operations[operation_id] = op_data
        save_operation(operation_id, op_data)
        
        return response