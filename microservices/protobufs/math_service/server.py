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
from service import MathService, process_async_operation, async_operations
from mom_handler import MOMHandler, process_pending_operations

# Puerto por defecto para el servidor
DEFAULT_PORT = 50051

# Variable global para controlar el estado del servidor
server_active = True

# Función para procesar manualmente los archivos pendientes
def process_files_immediately():
    """
    Procesa directamente todas las operaciones pendientes en archivos
    """
    print("Procesando archivos pendientes inmediatamente...")
    operations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "operations")
    
    if not os.path.exists(operations_dir):
        print("Directorio de operaciones no encontrado")
        return
    
    processed_count = 0
    
    for filename in os.listdir(operations_dir):
        if not filename.endswith('.json'):
            continue
        
        op_id = filename[:-5]  # Quitar extensión .json
        file_path = os.path.join(operations_dir, filename)
        
        try:
            with open(file_path, 'r') as f:
                op_data = json.load(f)
            
            # Verificar si es una operación pendiente
            if op_data.get("status") == 1:  # PENDING
                # Extraer a y b de la operación
                a = op_data.get("a")
                b = op_data.get("b")
                
                if a is not None and b is not None:
                    print(f"Procesando operación pendiente: {op_id} (suma de {a} + {b})")
                    
                    # Procesar la operación
                    process_async_operation(op_id, a, b)
                    processed_count += 1
                else:
                    print(f"Operación {op_id} incompleta, faltan valores a o b")
            else:
                # Ya no está pendiente
                status_map = {
                    0: "DESCONOCIDO",
                    1: "PENDIENTE",
                    2: "PROCESANDO",
                    3: "COMPLETADO",
                    4: "FALLIDO",
                    5: "CANCELADO"
                }
                current_status = status_map.get(op_data.get("status", 0), "DESCONOCIDO")
                print(f"Operación {op_id} no está pendiente (Estado: {current_status})")
                
        except Exception as e:
            print(f"Error al procesar archivo {filename}: {str(e)}")
    
    print(f"Procesamiento completado. Se procesaron {processed_count} operaciones pendientes.")

class MathServiceWithFailover(MathService):
    """
    Implementación de MathService con mecanismo de failover
    """
    def __init__(self):
        super().__init__()
        # Inicializar el manejador MOM
        self.mom_handler = MOMHandler()
    
    def Sum(self, request, context):
        """
        Método Sum con failover
        """
        # Verificar si el servidor está activo (simulación de falla)
        if not server_active:
            print("Servidor en modo degradado, enviando a cola MOM")
            
            # Enviar operación a la cola MOM
            operation_id, message = self.mom_handler.enqueue_operation(
                a=request.a,
                b=request.b,
                operation_id=request.operation_id if request.operation_id else None
            )
            
            # Crear respuesta indicando que la operación fue encolada
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Servicio no disponible: {message}")
            
            return operation_pb2.OperationResponse(
                result=0,
                success=False,
                error_message=f"Operación encolada: {message}",
                operation_id=operation_id
            )
        
        # Si el servidor está activo, procesar normalmente
        return super().Sum(request, context)
    
    def CheckStatus(self, request, context):
        """
        Método CheckStatus actualizado para informar el estado del servidor
        """
        # Calcular tiempo de actividad
        uptime = int(time.time() - self.start_time)
        
        # Determinar el estado del servidor
        if server_active:
            status = operation_pb2.StatusResponse.ServiceStatus.RUNNING
            message = "Servicio funcionando correctamente"
        else:
            status = operation_pb2.StatusResponse.ServiceStatus.DEGRADED
            message = "Servicio en modo degradado, usando MOM para operaciones"
        
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
    service = MathServiceWithFailover()
    operation_pb2_grpc.add_MathServiceServicer_to_server(service, server)
    
    # Configurar puerto
    server_address = f'[::]:{DEFAULT_PORT}'
    server.add_insecure_port(server_address)
    
    # Iniciar servidor
    server.start()
    print(f"Servidor MathService iniciado. Escuchando en {server_address}")
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