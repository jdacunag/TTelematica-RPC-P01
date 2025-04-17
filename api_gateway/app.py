from flask import Flask, request, jsonify
import grpc
import uuid
import json
import sys
import os
import glob

# Asegurarnos de que podemos importar los módulos del servicio
sys.path.append(os.path.abspath('../microservices/protobufs/math_service'))

# Importar los módulos generados por protobuf
import operation_pb2
import operation_pb2_grpc

app = Flask(__name__)

# Configuración del servidor gRPC
GRPC_SERVER_ADDRESS = 'localhost:50051'

# Directorio donde se almacenan las operaciones persistentes
OPERATIONS_DIR = os.path.abspath('../microservices/protobufs/math_service/operations')

def get_grpc_stub():
    """
    Crea y retorna un stub gRPC para el servicio MathService
    """
    channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
    return operation_pb2_grpc.MathServiceStub(channel)

def get_operation_from_file(operation_id):
    """
    Lee una operación directamente desde el archivo de persistencia
    """
    file_path = os.path.join(OPERATIONS_DIR, f"{operation_id}.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                operation = json.load(f)
            return operation
        except Exception as e:
            return None
    return None

@app.route('/health', methods=['GET'])
def health_check():
    """
    Endpoint para verificar el estado del API Gateway
    """
    return jsonify({
        'status': 'UP',
        'message': 'API Gateway funcionando correctamente'
    })

@app.route('/service/status', methods=['GET'])
def service_status():
    """
    Endpoint para verificar el estado del servicio matemático
    """
    service_id = request.args.get('service_id', 'math_service_01')
    
    try:
        # Obtener stub gRPC
        stub = get_grpc_stub()
        
        # Crear solicitud de estado
        status_request = operation_pb2.StatusRequest(service_id=service_id)
        
        # Llamar al servicio gRPC
        response = stub.CheckStatus(status_request)
        
        # Convertir respuesta gRPC a formato JSON
        status_map = {
            0: 'UNKNOWN',
            1: 'RUNNING',
            2: 'DEGRADED',
            3: 'DOWN'
        }
        
        return jsonify({
            'status': status_map.get(response.status, 'UNKNOWN'),
            'message': response.message,
            'uptime': response.uptime,
            'service_id': service_id
        })
    
    except grpc.RpcError as e:
        # Manejar error de gRPC
        return jsonify({
            'status': 'DOWN',
            'message': f'Servicio no disponible: {e.details()}',
            'code': str(e.code())
        }), 503

@app.route('/math/sum', methods=['POST'])
def sum_operation():
    """
    Endpoint para realizar una suma
    """
    # Obtener datos de la solicitud
    data = request.json
    
    if not data:
        return jsonify({
            'error': 'Datos no proporcionados'
        }), 400
    
    a = data.get('a')
    b = data.get('b')
    
    if a is None or b is None:
        return jsonify({
            'error': 'Se requieren los parámetros "a" y "b"'
        }), 400
    
    # Verificar que los valores sean numéricos
    try:
        a = int(a)
        b = int(b)
    except ValueError:
        return jsonify({
            'error': 'Los valores de "a" y "b" deben ser numéricos'
        }), 400
    
    # Obtener ID de operación si se proporciona, o generar uno nuevo
    operation_id = data.get('operation_id', str(uuid.uuid4()))
    
    try:
        # Obtener stub gRPC
        stub = get_grpc_stub()
        
        # Crear solicitud gRPC
        request_proto = operation_pb2.OperationRequest(
            a=a,
            b=b,
            operation_id=operation_id
        )
        
        # Llamar al servicio gRPC
        response = stub.Sum(request_proto)
        
        # Verificar si fue exitoso o encolado
        if response.success:
            return jsonify({
                'result': response.result,
                'success': response.success,
                'operation_id': response.operation_id
            })
        else:
            # Si no fue exitoso, puede ser que se haya encolado
            return jsonify({
                'success': False,
                'message': response.error_message,
                'operation_id': response.operation_id,
                'status': 'QUEUED'
            }), 202  # Respuesta 202 Accepted
    
    except grpc.RpcError as e:
        # Manejar caso de servicio no disponible
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            # Guardar la operación directamente en un archivo si el servicio no está disponible
            operation_dir = OPERATIONS_DIR
            if not os.path.exists(operation_dir):
                os.makedirs(operation_dir)
            
            file_path = os.path.join(operation_dir, f"{operation_id}.json")
            operation_data = {
                "status": 1,  # PENDING
                "message": "Operación en cola (API Gateway)",
                "timestamp": request.json.get("timestamp", 0),
                "a": a,
                "b": b
            }
            
            try:
                with open(file_path, 'w') as f:
                    json.dump(operation_data, f)
                
                print(f"Operación {operation_id} guardada en disco por API Gateway")
            except Exception as file_error:
                print(f"Error al guardar operación: {str(file_error)}")
            
            return jsonify({
                'success': False,
                'message': 'Servicio temporalmente no disponible. La operación ha sido encolada.',
                'operation_id': operation_id,
                'status': 'QUEUED'
            }), 202  # Respuesta 202 Accepted
        
        # Otros errores gRPC
        return jsonify({
            'error': f'Error RPC: {e.details()}',
            'code': str(e.code())
        }), 500

@app.route('/math/operation/status/<operation_id>', methods=['GET'])
def operation_status(operation_id):
    """
    Endpoint para consultar el estado de una operación
    """
    try:
        # Intentar obtener stub gRPC y consultar servicio
        try:
            stub = get_grpc_stub()
            
            # Crear solicitud gRPC
            request_proto = operation_pb2.AsyncOperationRequest(
                operation_id=operation_id
            )
            
            # Llamar al servicio gRPC
            response = stub.GetAsyncOperationStatus(request_proto)
            
            # Mapear códigos de estado a texto
            status_map = {
                0: 'UNKNOWN',
                1: 'PENDING',
                2: 'PROCESSING',
                3: 'COMPLETED',
                4: 'FAILED',
                5: 'CANCELLED'
            }
            
            # Crear respuesta
            result = {
                'operation_id': operation_id,
                'status': status_map.get(response.status, 'UNKNOWN'),
                'message': response.message
            }
            
            # Agregar resultado si está disponible
            if response.result and response.status == 3:  # COMPLETED
                result['result'] = {
                    'value': response.result.result,
                    'success': response.result.success
                }
            
            return jsonify(result)
        
        except grpc.RpcError as e:
            # Si el servicio no está disponible, buscar en archivos
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                # Buscar operación en archivos persistentes
                operation = get_operation_from_file(operation_id)
                
                if operation:
                    # Mapear códigos de estado a texto
                    status_map = {
                        0: 'UNKNOWN',
                        1: 'PENDING',
                        2: 'PROCESSING',
                        3: 'COMPLETED',
                        4: 'FAILED',
                        5: 'CANCELLED'
                    }
                    
                    # Crear respuesta
                    result = {
                        'operation_id': operation_id,
                        'status': status_map.get(operation.get('status', 0), 'UNKNOWN'),
                        'message': operation.get('message', 'Sin mensaje'),
                        'source': 'file_storage'  # Indicar fuente de datos
                    }
                    
                    # Agregar resultado si está disponible
                    if 'result' in operation and operation.get('status') == 3:  # COMPLETED
                        result['result'] = {
                            'value': operation['result'].get('result', 0),
                            'success': operation['result'].get('success', False)
                        }
                    
                    return jsonify(result)
                else:
                    # Si no se encuentra en archivos, responder con estado desconocido
                    return jsonify({
                        'operation_id': operation_id,
                        'status': 'UNKNOWN',
                        'message': f'Operación no encontrada: {operation_id}',
                        'source': 'api_gateway'
                    })
            else:
                # Otros errores gRPC
                raise e
    
    except Exception as e:
    # Manejar error general
        error_code = 'UNKNOWN'
        if hasattr(e, 'code'):
            error_code = str(e.code())
        
        return jsonify({
            'error': f'Error al consultar estado: {str(e)}',
            'code': error_code
        }), 500

@app.route('/operations', methods=['GET'])
def list_operations():
    """
    Endpoint para listar todas las operaciones disponibles
    """
    try:
        # Buscar archivos de operaciones
        operations = []
        
        # Crear directorio si no existe
        if not os.path.exists(OPERATIONS_DIR):
            os.makedirs(OPERATIONS_DIR)
        
        # Listar archivos JSON
        operation_files = glob.glob(os.path.join(OPERATIONS_DIR, '*.json'))
        
        # Cargar cada operación
        for file_path in operation_files:
            try:
                operation_id = os.path.basename(file_path)[:-5]  # Quitar .json
                
                with open(file_path, 'r') as f:
                    operation_data = json.load(f)
                
                # Mapear códigos de estado a texto
                status_map = {
                    0: 'UNKNOWN',
                    1: 'PENDING',
                    2: 'PROCESSING',
                    3: 'COMPLETED',
                    4: 'FAILED',
                    5: 'CANCELLED'
                }
                
                # Agregar a lista de operaciones
                operations.append({
                    'operation_id': operation_id,
                    'status': status_map.get(operation_data.get('status', 0), 'UNKNOWN'),
                    'message': operation_data.get('message', 'Sin mensaje')
                })
            except Exception as e:
                print(f"Error al cargar operación {file_path}: {str(e)}")
        
        return jsonify({
            'count': len(operations),
            'operations': operations
        })
    
    except Exception as e:
        return jsonify({
            'error': f'Error al listar operaciones: {str(e)}'
        }), 500

if __name__ == '__main__':
    # Verificar directorio de operaciones
    if not os.path.exists(OPERATIONS_DIR):
        try:
            os.makedirs(OPERATIONS_DIR)
            print(f"Directorio de operaciones creado: {OPERATIONS_DIR}")
        except Exception as e:
            print(f"Error al crear directorio de operaciones: {str(e)}")
    
    app.run(host='0.0.0.0', port=5000, debug=True)