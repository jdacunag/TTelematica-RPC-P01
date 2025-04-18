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
from service import SubtractService, process_async_operation, async_operations

import sys
import os

# Añadir directorio raíz al path para importar el paquete common
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from common.mom import MOMHandler, handle_failover
# Puerto por defecto para el servidor
DEFAULT_PORT = 50052  # Puerto diferente al de suma (50051)

# Variable global para controlar el estado del servidor
server_active = True

# Función para procesar manualmente los archivos pendientes
def process_files_immediately():
    """
    Procesa directamente todas las operaciones pendientes en archivos
    """
    print("Solicitando procesamiento de archivos pendientes...")
    # Usar el manejador MOM para procesar archivos
    import service
    mom_handler = MOMHandler(service_module=service)
    count = mom_handler.recovery.process_files_immediately()
    print(f"Procesamiento completado. Se procesaron {count} operaciones pendientes.")

class SubtractServiceWithFailover(SubtractService):
    """
    Implementación de SubtractService con mecanismo de failover
    """
    def __init__(self):
        super().__init__()
        # Inicializar el manejador MOM con el módulo service
        import service
        self.service_module = service  # Guardar referencia al módulo service
        self.mom_handler = MOMHandler(service_module=service)
    
    def Subtract(self, request, context):
        """
        Método Subtract con failover
        """
        # Verificar si el servidor está activo (simulación de falla)
        if not server_active:
            print("Servidor en modo degradado, enviando a cola MOM")
            
            # Enviar operación a la cola MOM usando el handle_failover centralizado
            operation_id, message = handle_failover(request, service_module=self.service_module)
            
            # Crear respuesta indicando que la operación fue encolada
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Servicio no disponible: {message}")
            
            return operation_pb2.SubtractResponse(
                result=0,
                success=False,
                error_message=f"Operación encolada: {message}",
                operation_id=operation_id
            )
        
        # Si el servidor está activo, procesar normalmente
        return super().Subtract(request, context)
    
    def CheckStatus(self, request, context):
        """
        Método CheckStatus actualizado para informar el estado del servidor
        """
        # Calcular tiempo de actividad
        uptime = int(time.time() - self.start_time)
        
        # Determinar el estado del servidor
        if server_active:
            status = operation_pb2.StatusResponse.ServiceStatus.RUNNING
            message = "Servicio de resta funcionando correctamente"
        else:
            status = operation_pb2.StatusResponse.ServiceStatus.DEGRADED
            message = "Servicio de resta en modo degradado, usando MOM para operaciones"
        
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
    service = SubtractServiceWithFailover()
    operation_pb2_grpc.add_SubtractServiceServicer_to_server(service, server)
    
    # Configurar puerto
    server_address = f'[::]:{DEFAULT_PORT}'
    server.add_insecure_port(server_address)
    
    # Iniciar servidor
    server.start()
    print(f"Servidor SubtractService iniciado. Escuchando en {server_address}")
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