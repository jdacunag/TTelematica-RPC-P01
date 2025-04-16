import grpc
import time
import operation_pb2
import operation_pb2_grpc
import uuid

def run():
    # Crear un canal de comunicación con el servidor gRPC
    channel = grpc.insecure_channel('localhost:50051')
    
    # Crear un stub (cliente) para el servicio MathService
    stub = operation_pb2_grpc.MathServiceStub(channel)
    
    # Generar un ID único para la operación
    operation_id = str(uuid.uuid4())
    
    # Crear la solicitud de operación
    a = 15
    b = 8
    request = operation_pb2.OperationRequest(a=a, b=b, operation_id=operation_id)
    
    print(f"Enviando solicitud de suma: {a} + {b}")
    
    try:
        # Intentar llamar al método Sum del servicio
        response = stub.Sum(request)
        
        # Verificar si la operación fue encolada (failover)
        if not response.success and "encolada" in response.error_message:
            print("La operación fue encolada debido a que el servicio está degradado")
            print(f"ID de operación: {response.operation_id}")
            
            # Consultar periódicamente el estado de la operación
            check_async_operation_status(stub, response.operation_id)
        else:
            # Operación procesada correctamente
            print(f"Respuesta de suma: {response.result}")
            print(f"Éxito: {response.success}")
            print(f"Mensaje de error: {response.error_message}")
            print(f"ID de operación: {response.operation_id}")
    
    except grpc.RpcError as e:
        # Manejar error RPC
        print(f"Error RPC: {e.code()}")
        print(f"Detalles: {e.details()}")
        
        # Si el servicio está degradado, el mensaje de error contendrá el ID de operación
        if e.code() == grpc.StatusCode.UNAVAILABLE and "Operación encolada" in e.details():
            # Extraer ID de operación del mensaje de error
            operation_id = extract_operation_id(e.details())
            if operation_id:
                print(f"La operación fue encolada con ID: {operation_id}")
                # Consultar periódicamente el estado de la operación
                check_async_operation_status(stub, operation_id)
    
    # Verificar el estado del servicio
    check_service_status(stub)

def check_async_operation_status(stub, operation_id):
    """
    Consulta periódicamente el estado de una operación asíncrona
    """
    print(f"\nConsultando estado de operación asíncrona: {operation_id}")
    
    max_attempts = 10
    attempt = 0
    
    while attempt < max_attempts:
        attempt += 1
        
        try:
            # Crear solicitud de estado
            status_request = operation_pb2.AsyncOperationRequest(operation_id=operation_id)
            
            # Consultar estado
            status_response = stub.GetAsyncOperationStatus(status_request)
            
            # Mostrar estado actual
            status_text = get_status_text(status_response.status)
            print(f"Intento {attempt}: Estado = {status_text}, Mensaje: {status_response.message}")
            
            # Si la operación está completada o ha fallado, mostrar resultado
            if status_response.status in [3, 4]:  # COMPLETED or FAILED
                if status_response.result:
                    print(f"Resultado: {status_response.result.result}")
                    print(f"Éxito: {status_response.result.success}")
                    print(f"Mensaje: {status_response.result.error_message}")
                break
            
            # Esperar antes del siguiente intento
            time.sleep(2)
            
        except grpc.RpcError as e:
            print(f"Error al consultar estado: {e.code()}")
            print(f"Detalles: {e.details()}")
            time.sleep(2)
    
    if attempt >= max_attempts:
        print("Se alcanzó el número máximo de intentos sin obtener un resultado final")

def check_service_status(stub):
    """
    Verifica el estado del servicio
    """
    try:
        status_response = stub.CheckStatus(
            operation_pb2.StatusRequest(service_id="math_service_01")
        )
        
        # Convertir código de estado a texto
        status_text = "DESCONOCIDO"
        if status_response.status == 1:
            status_text = "ACTIVO"
        elif status_response.status == 2:
            status_text = "DEGRADADO"
        elif status_response.status == 3:
            status_text = "CAÍDO"
        
        print(f"\nEstado del servicio: {status_text}")
        print(f"Mensaje: {status_response.message}")
        print(f"Tiempo activo: {status_response.uptime} segundos")
    
    except grpc.RpcError as e:
        print(f"Error al verificar estado: {e.code()}")
        print(f"Detalles: {e.details()}")

def extract_operation_id(error_message):
    """
    Extrae el ID de operación de un mensaje de error
    """
    # Implementación simple, se puede mejorar con regex
    if "operation_id" in error_message:
        parts = error_message.split("operation_id:")
        if len(parts) > 1:
            return parts[1].strip()
    return None

def get_status_text(status_code):
    """
    Convierte código de estado a texto
    """
    status_map = {
        0: "DESCONOCIDO",
        1: "PENDIENTE",
        2: "PROCESANDO",
        3: "COMPLETADO",
        4: "FALLIDO",
        5: "CANCELADO"
    }
    return status_map.get(status_code, "DESCONOCIDO")

if __name__ == '__main__':
    run()