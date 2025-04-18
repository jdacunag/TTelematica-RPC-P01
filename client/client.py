import requests
import json
import time
import uuid
import sys

# URL base del API Gateway
API_GATEWAY_URL = "http://localhost:5000"

def check_service_status():
    """
    Verifica el estado del servicio a través del API Gateway
    """
    url = f"{API_GATEWAY_URL}/service/status"
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            print("\nEstado del servicio:")
            print(f"Status: {data['status']}")
            print(f"Mensaje: {data['message']}")
            print(f"Tiempo activo: {data.get('uptime', 'N/A')} segundos")
            return data['status']
        else:
            print(f"Error al verificar estado del servicio: {response.status_code}")
            print(response.text)
            return None
    
    except requests.RequestException as e:
        print(f"Error de conexión: {str(e)}")
        print("¿Está el API Gateway ejecutándose?")
        return None

def sum_operation(a, b):
    """
    Realiza una operación de suma a través del API Gateway
    """
    # Ruta actualizada para coincidir con el API Gateway
    url = f"{API_GATEWAY_URL}/sum"
    
    # Generar ID único para la operación
    operation_id = str(uuid.uuid4())
    
    # Crear payload
    payload = {
        "a": a,
        "b": b,
        "operation_id": operation_id,
        "timestamp": time.time()
    }
    
    try:
        print(f"\nEnviando solicitud de suma: {a} + {b}")
        response = requests.post(url, json=payload)
        
        if response.status_code in [200, 202]:
            data = response.json()
            
            # Verificar si la operación fue exitosa o encolada
            if response.status_code == 200 and data.get('success', False):
                print("Operación completada con éxito:")
                print(f"Resultado: {data['result']}")
                print(f"ID de operación: {data['operation_id']}")
            else:
                print("Operación encolada o pendiente:")
                print(f"Mensaje: {data.get('message', 'No hay mensaje')}")
                print(f"ID de operación: {data.get('operation_id', 'Desconocido')}")
                
                # Si la operación está encolada, consultar periódicamente el estado
                if data.get('status') == 'QUEUED':
                    check_operation_status(data.get('operation_id'))
            
            return data
        else:
            print(f"Error al realizar la operación: {response.status_code}")
            print(response.text)
            return None
    
    except requests.RequestException as e:
        print(f"Error de conexión: {str(e)}")
        print("¿Está el API Gateway ejecutándose?")
        return None

def check_operation_status(operation_id):
    """
    Consulta periódicamente el estado de una operación
    """
    # Ruta actualizada para coincidir con el API Gateway
    url = f"{API_GATEWAY_URL}/operation/status/{operation_id}"
    max_attempts = 3  # Limitado a solo 3 intentos
    attempt = 0
    
    print(f"\nConsultando estado de la operación: {operation_id}")
    print(f"Se realizarán como máximo {max_attempts} intentos")
    
    while attempt < max_attempts:
        attempt += 1
        
        try:
            print(f"Intento {attempt} de {max_attempts}...")
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'UNKNOWN')
                message = data.get('message', 'Sin mensaje')
                source = data.get('source', 'server')
                
                print(f"Estado = {status}, Mensaje: {message}")
                if source != 'server':
                    print(f"Fuente de datos: {source}")
                
                # Si la operación está completa o ha fallado, mostrar el resultado y salir
                if status in ['COMPLETED', 'FAILED']:
                    if 'result' in data:
                        print(f"Resultado: {data['result']['value']}")
                        print(f"Éxito: {data['result']['success']}")
                    return  # Salir de la función
                
                # Esperar antes del siguiente intento
                time.sleep(2)
            else:
                print(f"Error al consultar estado: {response.status_code}")
                print(response.text)
                
                # Si es un error de servidor, detenemos inmediatamente
                if response.status_code >= 500:
                    print("Error del servidor detectado. Cancelando intentos restantes.")
                    return  # Salir inmediatamente si hay error de servidor
                
                time.sleep(2)
        
        except requests.RequestException as e:
            print(f"Error de conexión: {str(e)}")
            print("¿Está el API Gateway ejecutándose?")
            # No seguir intentando si hay problemas de conexión
            return
    
    print("\nSe alcanzó el número máximo de intentos sin obtener un resultado final")
    print("La operación podría seguir procesándose en segundo plano")
    print(f"Puede consultar su estado más tarde con el ID: {operation_id}")

def list_operations():
    """
    Lista todas las operaciones disponibles
    """
    url = f"{API_GATEWAY_URL}/operations"
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            operations = data.get('operations', [])
            
            if operations:
                print("\nOperaciones disponibles:")
                for i, op in enumerate(operations, 1):
                    print(f"{i}. ID: {op['operation_id']}")
                    print(f"   Estado: {op['status']}")
                    print(f"   Mensaje: {op['message']}")
                    print("---")
                
                print(f"Total: {data.get('count', len(operations))} operaciones")
            else:
                print("\nNo hay operaciones disponibles")
            
            return operations
        else:
            print(f"Error al listar operaciones: {response.status_code}")
            print(response.text)
            return None
    
    except requests.RequestException as e:
        print(f"Error de conexión: {str(e)}")
        print("¿Está el API Gateway ejecutándose?")
        return None

def interactive_mode():
    """
    Modo interactivo para el cliente
    """
    print("=== Cliente REST para Servicio Matemático ===")
    
    while True:
        print("\nOpciones:")
        print("1. Verificar estado del servicio")
        print("2. Realizar operación de suma")
        print("3. Consultar estado de operación")
        print("4. Listar todas las operaciones")
        print("5. Salir")
        
        choice = input("\nSeleccione una opción (1-5): ")
        
        if choice == '1':
            check_service_status()
        
        elif choice == '2':
            try:
                a = int(input("Ingrese el primer número: "))
                b = int(input("Ingrese el segundo número: "))
                sum_operation(a, b)
            except ValueError:
                print("Error: Debe ingresar valores numéricos")
        
        elif choice == '3':
            operation_id = input("Ingrese el ID de la operación: ")
            check_operation_status(operation_id)
        
        elif choice == '4':
            operations = list_operations()
            if operations and len(operations) > 0:
                check_specific = input("\n¿Desea consultar el estado de alguna operación? (s/n): ")
                if check_specific.lower() == 's':
                    try:
                        # En el código del cliente, cuando se procesa la selección del usuario
                        index = int(input("Ingrese el número de la operación: ")) - 1  # Restar 1 para ajustar al índice basado en 0
                        if 0 <= index < len(operations):
                            check_operation_status(operations[index]['operation_id'])
                        else:
                            print("Índice inválido")
                    except ValueError:
                        print("Debe ingresar un número válido")
        
        elif choice == '5':
            print("Saliendo...")
            break
        
        else:
            print("Opción no válida. Intente nuevamente.")

if __name__ == "__main__":
    # Si se proporcionan argumentos, ejecutar operación directamente
    if len(sys.argv) > 2:
        try:
            a = int(sys.argv[1])
            b = int(sys.argv[2])
            sum_operation(a, b)
        except ValueError:
            print("Error: Los argumentos deben ser numéricos")
    else:
        # Modo interactivo
        interactive_mode()