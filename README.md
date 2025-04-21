# Sistema Distribuido con Microservicios y Failover

Este proyecto implementa un sistema distribuido con microservicios que se comunican mediante gRPC y utilizan un mecanismo de failover basado en Message-Oriented Middleware (RabbitMQ).

# Info de la materia: Tópicos Especiales en Telemática

# Estudiante(s): nombre, email-eafit
| Nombre | email-EAFIT |
|--------|-------------|
| David Lopera Londoño | dloperal2@eafit.edu.co |
| Camilo Monsalve Montes | cmonsalvem@eafit.edu.co |
| Juan Diego Acuña Giraldo | jdacunag@eafit.edu.co |
#
# Profesor: nombre, email-eafit
| Profesor | email-EAFIT |
|----------|-------------|
| Edwin Nelson Montoya Múnera | emontoya@eafit.edu.co |

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
|   ├── Dockerfile.api-gateway     # Dockerfile para API Gateway
├── client/
│   ├── client.py                  # Cliente REST
|   ├── Dockerfile.client          # Dockerfile para el Cliente REST
├── common/
│   └── db/
│       ├── config.py              # Archivo de configuración de variables de entorno, URLs, etc
│       ├── operations_db.py       # Archivo que maneja la conexión a MongoDB Atlas
│   └── mom/
├── microservices/
│   └── protobufs/
│           ├── operation.proto    # Definición de servicios y mensajes
│           ├── operation_pb2.py   # Código generado por protoc
│           ├── operation_pb2_grpc.py  # Código generado por protoc
│   └── sum_service/
│           ├── server.py                # Servidor ejecutabke del microservicio de sumar
│           ├── service.py               # Servicio del microservicio de sumar
│           ├── Dockerfile.sum-service   # Dockerfile para el microservicio de sumar
│   └── substract_service/
│           ├── server.py                      # Servidor ejecutabke del microservicio de restar
│           ├── service.py                     # Servicio del microservicio de restar
│           ├── Dockerfile.substract-service   # Dockerfile para el microservicio de restar
│   └── mult_service/
│           ├── server.py                 # Servidor ejecutabke del microservicio de multiplicar
│           ├── service.py                # Servicio del microservicio de multiplicar
│           ├── Dockerfile.mult-service   # Dockerfile para el microservicio de multiplicar
├── service_monitor.py                       # Script para verificar el estado de MongoDB Atlas y RabbitMQ
├── test_mongodb_connection.py               # Archivo para testear la conexión a MongoDB Atlas y RabbitMQ
├── requirements.txt                         # Dependencias del proyecto
├── docker-compose.yml                       # Archivo docker-compose para crear contenedores, imagenes de cada parte del proyecto
├── README.md                      # Documentación del proyecto
└── SETUP.md                       # Guía de instalación
```

## Instalación y Configuración

### Requisitos Previos

- Python 3.8 o superior
- RabbitMQ
- Dependencias de cada componente (ver archivos requirements.txt)
- Docker y Docker-Compose

# Descripción general del ambiente de desarrollo técnico

## Detalles del desarrollo y técnicos

- **Python**: Lenguaje principal de programación (3.8 o superior)
- **Flask**: Framework para el API Gateway
- **gRPC**: Framework para comunicación entre microservicios
- **Protocol Buffers**: Mecanismo para serializar datos estructurados
- **RabbitMQ**: Sistema de mensajería para el mecanismo de failover
- **Pika**: Es una librería cliente de RabbitMQ para Python.
- **Requests**: Biblioteca para el cliente REST
- **MongoDB Atlas**: Base de datos noSQL para almacenar las operaciones en colecciones

 ```bash
   flask==2.3.3
   grpcio==1.60.1
   grpcio-tools==1.60.1
   requests==2.31.0
   pika==1.3.2
   pymongo==4.6.2
   dnspython==2.6.1
   python-dotenv==1.0.1
   ```


## Ejecución del Sistema

1. **Iniciar RabbitMQ**:
   ```bash
   # Usando Docker /en la raiz del proyecto)
   sudo docker run -d --hostname my-rabbit --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:management
   ```
2. **Crear el entorno virtual**:
   ```bash
   # Ejecutar en la raiz del proyecto
   python3 -m venv
   # Se creará una carpeta con los entornos virtuales "venv" en la raiz del proyecto 
   ```

   ```bash
   # Activar el entorno virtual
   source venv/bin/activate
   ```

3. **Instalar los requerimientos:**
   
   ```bash
   pip install -r requirements.txt
   ```

4. **Iniciar los Microservicios**:

   En diferentes terminales ejecutar
   
   ```bash
   cd microservices/sum_service
   python server.py
   ```

   ```bash
   cd microservices/substract_service
   python server.py
   ```

   ```bash
   cd microservices/mult_service
   python server.py
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

- **Python**: Lenguaje principal de programación (3.8 o superior)
- **Flask**: Framework para el API Gateway
- **gRPC**: Framework para comunicación entre microservicios
- **Protocol Buffers**: Mecanismo para serializar datos estructurados
- **RabbitMQ**: Sistema de mensajería para el mecanismo de failover
- **Pika**: Es una librería cliente de RabbitMQ para Python.
- **Requests**: Biblioteca para el cliente REST
- **MongoDB Atlas**: Base de datos noSQL para almacenar las operaciones en colecciones
