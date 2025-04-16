import time
import uuid
import operation_pb2
import operation_pb2_grpc

# Diccionario para almacenar operaciones asíncronas
async_operations = {}

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
        
        # Verificar si la operación existe
        if operation_id in async_operations:
            operation = async_operations[operation_id]
            return operation_pb2.AsyncOperationResponse(
                status=operation["status"],
                result=operation["result"] if "result" in operation else None,
                message=operation["message"]
            )
        else:
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
    async_operations[operation_id] = {
        "status": operation_pb2.AsyncOperationResponse.OperationStatus.PENDING,
        "message": "Operación en cola"
    }
    
    try:
        # Simular procesamiento
        time.sleep(2)
        
        # Actualizar estado a procesando
        async_operations[operation_id] = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.PROCESSING,
            "message": "Procesando operación"
        }
        
        # Realizar la operación
        result = a + b
        
        # Crear resultado y actualizar estado a completado
        response = operation_pb2.OperationResponse(
            result=result,
            success=True,
            error_message="",
            operation_id=operation_id
        )
        
        async_operations[operation_id] = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.COMPLETED,
            "message": "Operación completada",
            "result": response
        }
        
        return response
    
    except Exception as e:
        # En caso de error, actualizar estado a fallido
        response = operation_pb2.OperationResponse(
            result=0,
            success=False,
            error_message=str(e),
            operation_id=operation_id
        )
        
        async_operations[operation_id] = {
            "status": operation_pb2.AsyncOperationResponse.OperationStatus.FAILED,
            "message": f"Error al procesar: {str(e)}",
            "result": response
        }
        
        return response