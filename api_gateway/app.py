from flask import Flask, request, jsonify
import grpc
import uuid
import sys
import os
import time
import importlib

# Configurar rutas para importaciones
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(project_root)
sys.path.append(os.path.abspath('../microservices/sum_service'))
sys.path.append(os.path.abspath('../microservices/subtract_service'))
sys.path.append(os.path.abspath('../microservices/mult_service'))
sys.path.append(os.path.abspath('../microservices/protobufs'))

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

# Importar la clase OperationsDB
from common.db.operations_db import OperationsDB

# IMPORTANTE: Crear la instancia de Flask ANTES de definir rutas
app = Flask(__name__)

# Inicializar la conexión a MongoDB Atlas
db = OperationsDB.get_instance()

# Importar los módulos generados por protobuf
import operation_pb2
import operation_pb2_grpc

# Configuración para múltiples servicios
SERVICES = {
    'sum': {
        'stub_module': 'operation_pb2_grpc',
        'stub_class': 'SumServiceStub',
        'path': os.path.abspath('../microservices/sum_service'),
        'server_address': os.getenv('SUM_SERVICE_ADDRESS', 'localhost:50051'),
        'service_id': 'sum_service_01'
    },
    'subtract': {
        'stub_module': 'operation_pb2_grpc',
        'stub_class': 'SubtractServiceStub',
        'path': os.path.abspath('../microservices/subtract_service'),
        'server_address': os.getenv('SUBTRACT_SERVICE_ADDRESS', 'localhost:50052'),
        'service_id': 'subtract_service_01'
    },
    'mult': {
        'stub_module': 'operation_pb2_grpc',
        'stub_class': 'MultServiceStub',
        'path': os.path.abspath('../microservices/mult_service'),
        'server_address': os.getenv('MULT_SERVICE_ADDRESS', 'localhost:50053'),
        'service_id': 'mult_service_01'
    }
    # Aquí se pueden añadir más servicios en el futuro
}

def get_service_stub(service_name):
    """
    Obtiene el stub para un servicio específico
    """
    if service_name not in SERVICES:
        raise ValueError(f"Servicio desconocido: {service_name}")
    
    service_config = SERVICES[service_name]
    
    # Asegurar que el path del servicio está en sys.path
    if service_config['path'] not in sys.path:
        sys.path.append(service_config['path'])
    
    # Importar el módulo stub
    try:
        module = importlib.import_module(service_config['stub_module'])
        stub_class = getattr(module, service_config['stub_class'])
    except (ImportError, AttributeError) as e:
        print(f"Error al importar el stub: {e}")
        raise ValueError(f"Error al cargar el servicio {service_name}: {e}")
    
    # Crear y retornar el stub
    channel = grpc.insecure_channel(service_config['server_address'])
    return stub_class(channel)

def get_operation_from_db(operation_id):
    """
    Lee una operación desde MongoDB Atlas
    """
    return db.get_operation(operation_id)

def get_status_text(status_code):
    """
    Convierte códigos de estado numéricos a texto
    """
    status_map = {
        0: 'UNKNOWN',
        1: 'PENDING',
        2: 'PROCESSING',
        3: 'COMPLETED',
        4: 'FAILED',
        5: 'CANCELLED'
    }
    return status_map.get(status_code, 'UNKNOWN')

@app.route('/health', methods=['GET'])
def health_check():
    """
    Endpoint para verificar el estado del API Gateway
    """
    return jsonify({
        'status': 'UP',
        'message': 'API Gateway funcionando correctamente',
        'database': 'MongoDB Atlas' if db.is_connected() else 'Sin conexión a MongoDB'
    })

@app.route('/service/status', methods=['GET'])
def service_status():
    """
    Endpoint para verificar el estado del servicio
    """
    service_name = request.args.get('service', 'sum')  # Por defecto, consultar el servicio de suma
    
    try:
        # Verificar si el servicio existe
        if service_name not in SERVICES:
            return jsonify({
                'status': 'UNKNOWN',
                'message': f'Servicio desconocido: {service_name}'
            }), 404
        
        # Obtener la configuración del servicio
        service_config = SERVICES[service_name]
        service_id = service_config.get('service_id', f'{service_name}_service_01')
        
        # Obtener stub para el servicio
        stub = get_service_stub(service_name)
        
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
            'service_id': service_id,
            'service_name': service_name
        })
    
    except grpc.RpcError as e:
        # Manejar error de gRPC
        return jsonify({
            'status': 'DOWN',
            'message': f'Servicio no disponible: {e.details()}',
            'code': str(e.code()),
            'service_name': service_name
        }), 503
    
    except Exception as e:
        # Manejar otros errores
        return jsonify({
            'status': 'ERROR',
            'message': f'Error al consultar estado: {str(e)}',
            'service_name': service_name
        }), 500

@app.route('/sum', methods=['POST'])
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
    
    # Extraer y validar los operandos
    a_value = data.get('a')
    b_value = data.get('b')
    
    if a_value is None or b_value is None:
        return jsonify({
            'error': 'Se requieren los parámetros "a" y "b"'
        }), 400
    
    # Verificar que los valores sean numéricos
    try:
        a_value = int(a_value)
        b_value = int(b_value)
    except ValueError:
        return jsonify({
            'error': 'Los valores de "a" y "b" deben ser numéricos'
        }), 400
    
    # Obtener ID de operación si se proporciona, o generar uno nuevo
    operation_id = data.get('operation_id', str(uuid.uuid4()))
    
    try:
        # Obtener stub para el servicio de suma
        stub = get_service_stub('sum')
        
        # Crear solicitud gRPC
        request_proto = operation_pb2.SumRequest(
            a=a_value,
            b=b_value,
            operation_id=operation_id
        )
        
        # Llamar al servicio gRPC
        response = stub.Sum(request_proto)
        
        # Solicitar estado para forzar persistencia
        try:
            status_request = operation_pb2.AsyncOperationRequest(
                operation_id=operation_id
            )
            stub.GetAsyncOperationStatus(status_request)
        except Exception as status_error:
            print(f"Error al verificar estado después de suma: {str(status_error)}")
        
        # Verificar si fue exitoso o encolado
        if response.success:
            return jsonify({
                'result': response.result,
                'success': response.success,
                'operation_id': response.operation_id,
                'service': 'sum'
            })
        else:
            # Si no fue exitoso, puede ser que se haya encolado
            return jsonify({
                'success': False,
                'message': response.error_message,
                'operation_id': response.operation_id,
                'status': 'QUEUED',
                'service': 'sum'
            }), 202  # Respuesta 202 Accepted
    
    except grpc.RpcError as e:
        # Manejar caso de servicio no disponible
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            # Guardar la operación en MongoDB Atlas
            operation_data = {
                "status": 1,  # PENDING
                "message": "Operación en cola (API Gateway)",
                "timestamp": time.time(),
                "a": a_value,
                "b": b_value,
                "service": "sum",
                "operation_id": operation_id
            }
            
            # Guardar en MongoDB Atlas
            db.save_operation(operation_id, operation_data)
            
            return jsonify({
                'success': False,
                'message': 'Servicio temporalmente no disponible. La operación ha sido encolada.',
                'operation_id': operation_id,
                'status': 'QUEUED',
                'service': 'sum'
            }), 202  # Respuesta 202 Accepted
        
        # Otros errores gRPC
        return jsonify({
            'error': f'Error RPC: {e.details()}',
            'code': str(e.code())
        }), 500
    
    except Exception as e:
        # Manejar otros errores
        return jsonify({
            'error': f'Error general: {str(e)}'
        }), 500

@app.route('/subtract', methods=['POST'])
def subtract_operation():
    """
    Endpoint para realizar una resta
    """
    # Obtener datos de la solicitud
    data = request.json
    
    if not data:
        return jsonify({
            'error': 'Datos no proporcionados'
        }), 400
    
    # Extraer y validar los operandos
    a_value = data.get('a')
    b_value = data.get('b')
    
    if a_value is None or b_value is None:
        return jsonify({
            'error': 'Se requieren los parámetros "a" y "b"'
        }), 400
    
    # Verificar que los valores sean numéricos
    try:
        a_value = int(a_value)
        b_value = int(b_value)
    except ValueError:
        return jsonify({
            'error': 'Los valores de "a" y "b" deben ser numéricos'
        }), 400
    
    # Obtener ID de operación si se proporciona, o generar uno nuevo
    operation_id = data.get('operation_id', str(uuid.uuid4()))
    
    try:
        # Obtener stub para el servicio de resta
        stub = get_service_stub('subtract')
        
        # Crear solicitud gRPC
        request_proto = operation_pb2.SubtractRequest(
            a=a_value,
            b=b_value,
            operation_id=operation_id
        )
        
        # Llamar al servicio gRPC
        response = stub.Subtract(request_proto)
        
        # Solicitar estado para forzar persistencia
        try:
            status_request = operation_pb2.AsyncOperationRequest(
                operation_id=operation_id
            )
            stub.GetAsyncOperationStatus(status_request)
        except Exception as status_error:
            print(f"Error al verificar estado después de resta: {str(status_error)}")
        
        # Verificar si fue exitoso o encolado
        if response.success:
            return jsonify({
                'result': response.result,
                'success': response.success,
                'operation_id': response.operation_id,
                'service': 'subtract'
            })
        else:
            # Si no fue exitoso, puede ser que se haya encolado
            return jsonify({
                'success': False,
                'message': response.error_message,
                'operation_id': response.operation_id,
                'status': 'QUEUED',
                'service': 'subtract'
            }), 202  # Respuesta 202 Accepted
    
    except grpc.RpcError as e:
        # Manejar caso de servicio no disponible
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            # Guardar la operación en MongoDB Atlas
            operation_data = {
                "status": 1,  # PENDING
                "message": "Operación en cola (API Gateway)",
                "timestamp": time.time(),
                "a": a_value,
                "b": b_value,
                "service": "subtract",
                "operation_id": operation_id
            }
            
            # Guardar en MongoDB Atlas
            db.save_operation(operation_id, operation_data)
            
            return jsonify({
                'success': False,
                'message': 'Servicio temporalmente no disponible. La operación ha sido encolada.',
                'operation_id': operation_id,
                'status': 'QUEUED',
                'service': 'subtract'
            }), 202  # Respuesta 202 Accepted
        
        # Otros errores gRPC
        return jsonify({
            'error': f'Error RPC: {e.details()}',
            'code': str(e.code())
        }), 500
    
    except Exception as e:
        # Manejar otros errores
        return jsonify({
            'error': f'Error general: {str(e)}'
        }), 500

@app.route('/multiply', methods=['POST'])
def multiply_operation():
    """
    Endpoint para realizar una multiplicación
    """
    # Obtener datos de la solicitud
    data = request.json
    
    if not data:
        return jsonify({
            'error': 'Datos no proporcionados'
        }), 400
    
    # Extraer y validar los operandos
    a_value = data.get('a')
    b_value = data.get('b')
    
    if a_value is None or b_value is None:
        return jsonify({
            'error': 'Se requieren los parámetros "a" y "b"'
        }), 400
    
    # Verificar que los valores sean numéricos
    try:
        a_value = int(a_value)
        b_value = int(b_value)
    except ValueError:
        return jsonify({
            'error': 'Los valores de "a" y "b" deben ser numéricos'
        }), 400
    
    # Obtener ID de operación si se proporciona, o generar uno nuevo
    operation_id = data.get('operation_id', str(uuid.uuid4()))
    
    try:
        # Obtener stub para el servicio de multiplicación
        stub = get_service_stub('multiply')
        
        # Crear solicitud gRPC
        request_proto = operation_pb2.MultiplyRequest(
            a=a_value,
            b=b_value,
            operation_id=operation_id
        )
        
        # Llamar al servicio gRPC
        response = stub.Multiply(request_proto)
        
        # Solicitar estado para forzar persistencia
        try:
            status_request = operation_pb2.AsyncOperationRequest(
                operation_id=operation_id
            )
            stub.GetAsyncOperationStatus(status_request)
        except Exception as status_error:
            print(f"Error al verificar estado después de multiplicación: {str(status_error)}")
        
        # Verificar si fue exitoso o encolado
        if response.success:
            return jsonify({
                'result': response.result,
                'success': response.success,
                'operation_id': response.operation_id,
                'service': 'multiply'
            })
        else:
            # Si no fue exitoso, puede ser que se haya encolado
            return jsonify({
                'success': False,
                'message': response.error_message,
                'operation_id': response.operation_id,
                'status': 'QUEUED',
                'service': 'multiply'
            }), 202  # Respuesta 202 Accepted
    
    except grpc.RpcError as e:
        # Manejar caso de servicio no disponible
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            # Guardar la operación en MongoDB Atlas
            operation_data = {
                "status": 1,  # PENDING
                "message": "Operación en cola (API Gateway)",
                "timestamp": time.time(),
                "a": a_value,
                "b": b_value,
                "service": "multiply",
                "operation_id": operation_id
            }
            
            # Guardar en MongoDB Atlas
            db.save_operation(operation_id, operation_data)
            
            return jsonify({
                'success': False,
                'message': 'Servicio temporalmente no disponible. La operación ha sido encolada.',
                'operation_id': operation_id,
                'status': 'QUEUED',
                'service': 'multiply'
            }), 202  # Respuesta 202 Accepted
        
        # Otros errores gRPC
        return jsonify({
            'error': f'Error RPC: {e.details()}',
            'code': str(e.code())
        }), 500
    
    except Exception as e:
        # Manejar otros errores
        return jsonify({
            'error': f'Error general: {str(e)}'
        }), 500

@app.route('/mult', methods=['POST'])
def mult_operation():
    """
    Endpoint para realizar una multiplicación
    """
    # Obtener datos de la solicitud
    data = request.json
    
    if not data:
        return jsonify({
            'error': 'Datos no proporcionados'
        }), 400
    
    # Extraer y validar los operandos
    a_value = data.get('a')
    b_value = data.get('b')
    
    if a_value is None or b_value is None:
        return jsonify({
            'error': 'Se requieren los parámetros "a" y "b"'
        }), 400
    
    # Verificar que los valores sean numéricos
    try:
        a_value = int(a_value)
        b_value = int(b_value)
    except ValueError:
        return jsonify({
            'error': 'Los valores de "a" y "b" deben ser numéricos'
        }), 400
    
    # Obtener ID de operación si se proporciona, o generar uno nuevo
    operation_id = data.get('operation_id', str(uuid.uuid4()))
    
    try:
        # Obtener stub para el servicio de multiplicación
        stub = get_service_stub('mult')
        
        # Crear solicitud gRPC
        request_proto = operation_pb2.MultRequest(
            a=a_value,
            b=b_value,
            operation_id=operation_id
        )
        
        # Llamar al servicio gRPC
        response = stub.Mult(request_proto)
        
        # Solicitar estado para forzar persistencia
        try:
            status_request = operation_pb2.AsyncOperationRequest(
                operation_id=operation_id
            )
            stub.GetAsyncOperationStatus(status_request)
        except Exception as status_error:
            print(f"Error al verificar estado después de multiplicación: {str(status_error)}")
        
        # IMPORTANTE: Si la operación fue exitosa, guardarla explícitamente en el filesystem
        if response.success:
            try:
                # Crear directorio si no existe
                if not os.path.exists(OPERATIONS_DIR):
                    os.makedirs(OPERATIONS_DIR)
                
                # Crear datos de la operación
                operation_data = {
                    "status": 3,  # COMPLETED
                    "message": "Operación completada",
                    "result": {
                        "result": response.result,
                        "success": response.success,
                        "error_message": response.error_message if hasattr(response, 'error_message') else "",
                        "operation_id": operation_id
                    },
                    "timestamp": time.time(),
                    "service": "mult"  # Identificar el servicio que realizó la operación
                }
                
                # Guardar en archivo
                file_path = os.path.join(OPERATIONS_DIR, f"{operation_id}.json")
                with open(file_path, 'w') as f:
                    json.dump(operation_data, f)
                
                print(f"Operación exitosa {operation_id} guardada en {file_path}")
            except Exception as save_error:
                print(f"Error al guardar operación exitosa: {str(save_error)}")
        
        # Verificar si fue exitoso o encolado
        if response.success:
            return jsonify({
                'result': response.result,
                'success': response.success,
                'operation_id': response.operation_id,
                'service': 'mult'
            })
        else:
            # Si no fue exitoso, puede ser que se haya encolado
            return jsonify({
                'success': False,
                'message': response.error_message,
                'operation_id': response.operation_id,
                'status': 'QUEUED',
                'service': 'mult'
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
                "timestamp": request.json.get("timestamp", time.time()),
                "a": a_value,
                "b": b_value,
                "service": "mult"  # Identificar el servicio que debería procesar la operación
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
                'status': 'QUEUED',
                'service': 'mult'
            }), 202  # Respuesta 202 Accepted
        
        # Otros errores gRPC
        return jsonify({
            'error': f'Error RPC: {e.details()}',
            'code': str(e.code())
        }), 500

@app.route('/operation/status/<operation_id>', methods=['GET'])
def operation_status(operation_id):
    """
    Endpoint para consultar el estado de una operación
    """
    # Intentar obtener información del servicio desde los parámetros
    service_name = request.args.get('service', None)
    
    # Si no se especifica servicio, intentar detectarlo desde MongoDB
    if not service_name:
        operation = db.get_operation(operation_id)
        if operation:
            service_name = operation.get('service', 'sum')
        else:
            service_name = 'sum'  # Por defecto servicio de suma
    
    try:
        # Intentar obtener stub gRPC y consultar servicio
        try:
            stub = get_service_stub(service_name)
            
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
                'message': response.message,
                'service': service_name,
                'source': 'microservice'
            }
            
            # Agregar resultado si está disponible
            if hasattr(response, 'result') and response.result and response.status == 3:  # COMPLETED
                result['result'] = {
                    'value': response.result.result,
                    'success': response.result.success
                }
            
            return jsonify(result)
        
        except grpc.RpcError as e:
            # Si el servicio no está disponible, buscar en MongoDB Atlas
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                # Buscar operación en MongoDB Atlas
                operation = db.get_operation(operation_id)
                
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
                    
                    # Detectar servicio desde MongoDB
                    db_service = operation.get('service', service_name)
                    
                    # Crear respuesta
                    result = {
                        'operation_id': operation_id,
                        'status': status_map.get(operation.get('status', 0), 'UNKNOWN'),
                        'message': operation.get('message', 'Sin mensaje'),
                        'source': 'mongodb_atlas',
                        'service': db_service
                    }
                    
                    # Agregar resultado si está disponible
                    if 'result' in operation and operation.get('status') == 3:  # COMPLETED
                        result['result'] = {
                            'value': operation['result'].get('result', 0),
                            'success': operation['result'].get('success', False)
                        }
                    
                    return jsonify(result)
                else:
                    # Si no se encuentra en MongoDB Atlas
                    return jsonify({
                        'operation_id': operation_id,
                        'status': 'UNKNOWN',
                        'message': f'Operación no encontrada: {operation_id}',
                        'source': 'api_gateway',
                        'service': service_name
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
            'code': error_code,
            'service': service_name
        }), 500

@app.route('/operations', methods=['GET'])
def list_operations():
    """
    Endpoint para listar todas las operaciones disponibles
    """
    try:
        # Filtrar por servicio si se especifica
        service_filter = request.args.get('service')
        
        # Parámetros de paginación
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 100))
        skip = (page - 1) * limit
        
        # Obtener operaciones desde MongoDB Atlas
        operations = db.list_operations(
            service=service_filter,
            limit=limit,
            skip=skip,
            sort_by='timestamp',
            sort_dir=-1  # Ordenar por tiempo descendente (más recientes primero)
        )
        
        # Obtener conteo total
        total_count = db.count_operations(service=service_filter)
        
        # Mapear códigos de estado a texto
        status_map = {
            0: 'UNKNOWN',
            1: 'PENDING',
            2: 'PROCESSING',
            3: 'COMPLETED',
            4: 'FAILED',
            5: 'CANCELLED'
        }
        
        # Crear lista para presentación
        presentation_operations = []
        for op in operations:
            # Crear objeto de presentación con datos principales
            presentation_op = {
                'operation_id': op['operation_id'],
                'status': status_map.get(op.get('status', 0), 'UNKNOWN'),
                'message': op.get('message', 'Sin mensaje'),
                'service': op.get('service', 'unknown')
            }
            
            # Añadir datos adicionales si están disponibles
            if 'a' in op and 'b' in op:
                presentation_op['operands'] = {
                    'a': op['a'],
                    'b': op['b']
                }
            
            # Añadir resultado si está disponible
            if 'result' in op and op.get('status') == 3:  # COMPLETED
                presentation_op['result'] = op['result'].get('result', 0)
            
            # Añadir timestamp si está disponible (convertido a formato legible)
            if 'timestamp' in op:
                from datetime import datetime
                presentation_op['timestamp'] = datetime.fromtimestamp(op['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            
            presentation_operations.append(presentation_op)
        
        return jsonify({
            'count': len(presentation_operations),
            'total': total_count,
            'page': page,
            'limit': limit,
            'operations': presentation_operations,
            'source': 'mongodb_atlas',
            'service_filter': service_filter
        })
    
    except Exception as e:
        print(f"Error general en list_operations: {str(e)}")
        return jsonify({
            'error': f'Error al listar operaciones: {str(e)}'
        }), 500

if __name__ == '__main__':
    # Verificar conexión a MongoDB Atlas
    if db.is_connected():
        print("Conectado a MongoDB Atlas exitosamente")
    else:
        print("ADVERTENCIA: No hay conexión a MongoDB Atlas")
    
    # Iniciar servidor Flask
    app.run(host='0.0.0.0', port=5000, debug=True)