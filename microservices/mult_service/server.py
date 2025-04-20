import grpc
from concurrent import futures
import time
import signal
import sys
import threading
import os
import json
import operation_pb2
import operation_pb2_grpc
from service import MultService, process_async_operation, async_operations

# Añadir directorio raíz al path para importar el paquete common
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

from common.mom import MOMHandler, handle_failover
from common.db.operations_db import OperationsDB

# Inicializar la conexión a MongoDB Atlas
db = OperationsDB.get_instance()

# Puerto por defecto para el servidor
DEFAULT_PORT = 50053  # Puerto diferente al de suma (50051) y resta (50052)

# Variable global para controlar el estado del servidor
server_active = True

# Función para procesar manualmente los archivos pendientes
def process_files_immediately():
    """
    Procesa directamente todas las operaciones pendientes en archivos y en MongoDB
    """
    print("Solicitando procesamiento de operaciones pendientes...")
    
    # Usar el manejador MOM para procesar archivos
    import service
    mom_handler = MOMHandler(service_module=service)
    count_files = mom_handler.recovery.process_files_immediately()
    
    # También procesar operaciones pendientes en MongoDB
    try:
        # Obtener operaciones pendientes (status = PENDING) para este servicio
        pending_ops = db.get_pending_operations("mult")
        count_db = 0
        
        for op in pending_ops:
            try:
                # Extraer los datos necesarios
                operation_id = op["operation_id"]
                a = op["a"]
                b = op["b"]
                
                print(f"Procesando operación pendiente de MongoDB: {operation_id} (multiplicación de {a} * {b})")
                
                # Procesar la operación
                result = process_async_operation(operation_id, a, b)
                count_db += 1
                
                # Añadir pequeña pausa para no sobrecargar el sistema
                time.sleep(0.1)
            except Exception as e:
                print(f"Error al procesar operación pendiente de MongoDB {op.get('operation_id', 'unknown')}: {str(e)}")
        
        total_count = count_files + count_db
        print(f"Procesamiento completado. Se procesaron {total_count} operaciones pendientes.")
        print(f"- Desde archivos: {count_files}")
        print(f"- Desde MongoDB: {count_db}")
        
        return total_count
    except Exception as e:
        print(f"Error al procesar operaciones pendientes de MongoDB: {str(e)}")
        return count_files

class MultServiceWithFailover(MultService):
    """
    Implementación de MultService con mecanismo de failover que usa MongoDB
    """
    def __init__(self):
        super().__init__()
        # Inicializar el manejador MOM con el módulo service
        import service
        self.service_module = service  # Guardar referencia al módulo service
        self.mom_handler = MOMHandler(service_module=service)
    
    def Mult(self, request, context):
        """
        Método Mult con failover
        """
        # Verificar si el servidor está activo (simulación de falla)
        if not server_active:
            print("Servidor en modo degradado, enviando a cola MOM")
            
            # Enviar operación a la cola MOM usando el handle_failover centralizado
            operation_id, message = handle_failover(request, service_module=self.service_module)
            
            # Guardar también en MongoDB como pendiente
            try:
                operation_data = {
                    "status": operation_pb2.AsyncOperationResponse.OperationStatus.PENDING.value,
                    "message": "Operación en cola (Servidor degradado)",
                    "timestamp": time.time(),
                    "service": "mult",
                    "a": request.a,
                    "b": request.b,
                    "operation_id": operation_id
                }
                db.save_operation(operation_id, operation_data)
                print(f"Operación encolada guardada en MongoDB: {operation_id}")
            except Exception as db_error:
                print(f"Error al guardar operación en MongoDB: {str(db_error)}")
            
            # Crear respuesta indicando que la operación fue encolada
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Servicio no disponible: {message}")
            
            return operation_pb2.MultResponse(
                result=0,
                success=False,
                error_message=f"Operación encolada: {message}",
                operation_id=operation_id
            )
        
        # Si el servidor está activo, procesar normalmente
        return super().Mult(request, context)
    
    def CheckStatus(self, request, context):
        """
        Método CheckStatus actualizado para informar el estado del servidor
        """
        # Calcular tiempo de actividad
        uptime = int(time.time() - self.start_time)
        
        # Determinar el estado del servidor
        if server_active:
            status = operation_pb2.StatusResponse.ServiceStatus.RUNNING
            message = "Servicio de multiplicación funcionando correctamente"
        else:
            status = operation_pb2.StatusResponse.ServiceStatus.DEGRADED
            message = "Servicio de multiplicación en modo degradado, usando MOM para operaciones"
        
        # Verificar si el ID de servicio coincide
        if request.service_id == self.service_id:
            return operation_pb2.StatusResponse(
                status=status,
                message=message,
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
        Método GetAsyncOperationStatus que busca primero en MongoDB
        """
        operation_id = request.operation_id
        print(f"Consultando estado de operación asíncrona: {operation_id}")
        
        # Buscar primero en la base de datos
        operation = db.get_operation(operation_id)
        
        if operation:
            # Si se encontró en MongoDB, actualizar caché en memoria
            async_operations[operation_id] = operation
            
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
        
        # Si no está en MongoDB, usar la implementación original
        return super().GetAsyncOperationStatus(request, context)

def toggle_server_status():
    """
    Función para cambiar el estado del servidor (para simulación de fallos)
    """
    global server_active
    server_active = not server_active
    status = "ACTIVO" if server_active else "DEGRADADO"
    print(f"Estado del servidor cambiado a: {status}")
    
    # Si el servidor vuelve a estar activo, procesar operaciones pendientes
    if server_active:
        print("Servidor reactivado, procesando operaciones pendientes...")
        process_files_immediately()

def serve():
    # Crear un servidor gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar servicio con failover
    service = MultServiceWithFailover()
    operation_pb2_grpc.add_MultServiceServicer_to_server(service, server)
    
    # Configurar puerto
    server_address = f'[::]:{DEFAULT_PORT}'
    server.add_insecure_port(server_address)
    
    # Iniciar servidor
    server.start()
    print(f"Servidor MultService iniciado. Escuchando en {server_address}")
    print("Estado inicial: ACTIVO")
    
    # Configurar handler para comandos de consola
    def console_handler():
        global server_active
        while True:
            cmd = input("Comandos: 'toggle' para cambiar estado, 'process' para procesar operaciones pendientes, 'exit' para salir\n")
            if cmd.lower() == 'toggle':
                toggle_server_status()
            elif cmd.lower() == 'process':
                process_files_immediately()
            elif cmd.lower() == 'exit':
                print("Cerrando servidor...")
                server.stop(0)
                service.mom_handler.close()
                sys.exit(0)
    
    # Iniciar thread para comandos de consola
    console_thread = threading.Thread(target=console_handler)
    console_thread.daemon = True
    console_thread.start()
    
    # Configurar handler para señales de terminación
    def handle_shutdown(signum, frame):
        print("Recibida señal de terminación. Cerrando servidor...")
        server.stop(0)
        service.mom_handler.close()
        sys.exit(0)
    
    # Registrar handlers para SIGINT y SIGTERM
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    try:
        # Mantener el servidor ejecutándose
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        service.mom_handler.close()

if __name__ == '__main__':
    serve()