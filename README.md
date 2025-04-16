# Sistema Distribuido con Microservicios y Failover

Este proyecto implementa un sistema distribuido con microservicios que se comunican mediante gRPC y utilizan un mecanismo de failover basado en Message-Oriented Middleware (RabbitMQ).

## Arquitectura del Sistema

La arquitectura del sistema consta de los siguientes componentes:

1. **Cliente REST**: Aplicación cliente que se comunica con el sistema a través de peticiones REST.
2. **API Gateway**: Componente central que recibe las peticiones REST del cliente y las traduce a llamadas gRPC hacia los microservicios.
3. **Microservicio de Matemáticas**: Servicio que realiza operaciones matemáticas (suma) y proporciona información sobre su estado.
4. **Message-Oriented Middleware (MOM)**: Sistema de mensajería basado en RabbitMQ que permite implementar un mecanismo de failover para gestionar mensajes cuando un microservicio no está disponible.

## Estructura del Proyecto

```
TTelematica-RPC-P01/
├── api_gateway/
│   ├── app.py                     # Implementación del API Gateway
│   └── requirements.txt           # Dependencias del API Gateway
├── client/
│   ├── client.py                  # Cliente REST
│   └── requirements.txt           # Dependencias del cliente
├── microservices/
│   └── protobufs/
│       └── math_service/
│           ├── operation.proto    # Definición de servicios y mensajes
│           ├── operation_pb2.py   # Código generado por protoc
│           ├── operation_pb2_grpc.py  # Código generado por protoc
│           ├── server.py          # Servidor gRPC
│           ├── updated_server.py  # Servidor gRPC con failover
│           ├── service.py         # Implementación del servicio
│           ├── mom_handler.py     # Manejador de MOM
│           ├── test_client.py     # Cliente gRPC de prueba
│           ├── failover_client.py # Cliente gRPC con manejo de failover
│           └── requirements.txt   # Dependencias del microservicio
├── README.md                      # Documentación del proyecto
└── SETUP.md                       # Guía de instalación
```

## Instalación y Configuración

Ver [SETUP.md](SETUP.md) para instrucciones detalladas de instalación.

### Requisitos Previos

- Python 3.8 o superior
- RabbitMQ
- Dependencias de cada componente (ver archivos requirements.txt)

## Ejecución del Sistema

1. **Iniciar RabbitMQ**:
   ```bash
   # Usando Docker
   docker run -d --hostname my-rabbit --name rabbitmq -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password rabbitmq:3-management
   ```

2. **Iniciar el Microservicio**:
   ```bash
   cd microservices/protobufs/math_service
   python updated_server.py
   ```

3. **Iniciar el API Gateway**:
   ```bash
   cd api_gateway
   python app.py
   ```

4. **Ejecutar el Cliente REST**:
   ```bash
   cd client
   python client.py
   ```

## Prueba del Sistema

### Simulación de Failover

1. Inicie todos los componentes como se indica en la sección anterior.
2. Desde la consola del servidor (microservicio), escriba `toggle` para cambiar el estado del servidor a degradado (simulando una falla).
3. Realice una operación desde el cliente.
4. Observe cómo la operación se encola en RabbitMQ y el cliente recibe una notificación.
5. Escriba `toggle` nuevamente para restaurar el servidor.
6. Observe cómo la operación encolada es procesada.
7. Consulte el estado de la operación desde el cliente para verificar que se ha completado.

## Funcionalidades Implementadas

- Comunicación REST entre cliente y API Gateway
- Comunicación gRPC entre API Gateway y microservicios
- Failover mediante RabbitMQ cuando el microservicio no está disponible
- Consulta asíncrona del estado de operaciones
- Monitoreo del estado de los servicios

## Tecnologías Utilizadas

- **Python**: Lenguaje principal de programación
- **Flask**: Framework para el API Gateway
- **gRPC**: Framework para comunicación entre microservicios
- **Protocol Buffers**: Mecanismo para serializar datos estructurados
- **RabbitMQ**: Sistema de mensajería para el mecanismo de failover
- **Requests**: Biblioteca para el cliente REST