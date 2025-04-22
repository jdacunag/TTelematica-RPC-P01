# Sistema Distribuido con Microservicios y Failover

## Tópicos Especiales en Telemática

## Estudiante(s):
| Nombre | Correo |
|--------|-------------|
| David Lopera Londoño | dloperal2@eafit.edu.co |
| Camilo Monsalve Montes | cmonsalvem@eafit.edu.co |
| Juan Diego Acuña Giraldo | jdacunag@eafit.edu.co |

## Profesor:
| Profesor | Correo |
|----------|-------------|
| Edwin Nelson Montoya Múnera | emontoya@eafit.edu.co |

## 1. Descripción de la actividad

Este proyecto implementa un sistema distribuido basado en microservicios que ofrece operaciones matemáticas (suma, resta y multiplicación) a través de un API Gateway. El sistema utiliza gRPC para la comunicación entre microservicios y un mecanismo de failover mediante Message-Oriented Middleware (RabbitMQ) para garantizar la entrega de mensajes incluso cuando un servicio no está disponible temporalmente. También se implementa el almacenamiento persistente de operaciones mediante MongoDB Atlas.

## 1.2 Aspectos NO cumplidos o desarrollados

El proyecto ha implementado todos los requerimientos solicitados en la asignación del proyecto.

## 2. Información general del proyecto

### Arquitectura del Sistema

La arquitectura del sistema consta con los siguientes componentes:

1. **Cliente:** Aplicación en Python que consume los servicios a través de REST.
2. **API Gateway:** Componente Flask que expone endpoints REST y traduce las solicitudes a llamadas gRPC.
3. **Microservicios:** Servicios independientes para suma, resta y multiplicación, implementados con gRPC.
4. **Message-Oriented Middleware (MOM):** Componente basado en RabbitMQ para gestionar fallos.
5. **Base de datos MongoDB Atlas:** Para la persistencia de operaciones y su estado.

### Patrones implementados

- **API Gateway**: Centraliza las solicitudes y oculta la complejidad interna.
- **Remote Procedure Call (RPC)**: Mediante gRPC para la comunicación eficiente entre servicios.
- **Message Queuing**: Para el manejo de operaciones asíncronas y failover.
- **Circuit Breaker (simplificado)**: Detecta fallos y redirige operaciones a la cola de mensajes.
- **Repository Pattern**: Para la abstracción del acceso a datos (MongoDB).
- **Singleton**: Para manejar conexiones a bases de datos y recursos compartidos.

### Mejores prácticas

- **Separación de responsabilidades**: Cada componente tiene un propósito claro y definido.
- **Configuración externalizada**: Variables de entorno para configurar conexiones y parámetros.
- **Manejo de errores**: Identificación, registro y recuperación ante fallos.
- **Dockerización**: Contenedores para facilitar el despliegue y la portabilidad.
- **Logging**: Registro detallado de operaciones para facilitar la depuración.

## 3. Descripción del ambiente de desarrollo y técnico

### Lenguajes, librerías y paquetes

- **Python 3.8+**: Lenguaje principal de programación
- **Flask 2.3.3**: Framework para el API Gateway
- **gRPC 1.60.1**: Framework para comunicación entre microservicios
- **Protocol Buffers**: Mecanismo para serializar datos estructurados
- **RabbitMQ**: Sistema de mensajería para el mecanismo de failover
- **Pika 1.3.2**: Cliente de RabbitMQ para Python
- **Requests 2.31.0**: Biblioteca para el cliente REST
- **MongoDB Atlas (PyMongo 4.6.2)**: Base de datos NoSQL para almacenar las operaciones
- **Docker y Docker-Compose**: Para contenerización y orquestación

### Compilación y ejecución

#### Usando Docker Compose (recomendado)

1. Clona el repositorio y navega hasta el directorio:
   ```bash
   git clone https://github.com/jdacunag/TTelematica-RPC-P01.git
   cd TTelematica-RPC-P01
   ```

2. Ejecuta los servicios usando Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Para detener los servicios:
   ```bash
   docker-compose down
   ```

#### Ejecución manual (desarrollo)

1. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

2. Inicia los microservicios (en terminales separadas):
   ```bash
   # Terminal 1
   cd microservices/sum_service
   python server.py
   
   # Terminal 2
   cd microservices/substract_service
   python server.py
   
   # Terminal 3
   cd microservices/mult_service
   python server.py
   ```

3. Inicia el API Gateway:
   ```bash
   cd api_gateway
   python app.py
   ```

4. Ejecuta el cliente:
   ```bash
   cd client
   python client.py
   ```

### Configuración de parámetros

La configuración se realiza mediante un archivo `.env` con las siguientes variables:

#### MongoDB Atlas
```
MONGO_URI=mongodb+srv://usuario:password@cluster.mongodb.net/microservices_db?retryWrites=true&w=majority
MONGO_DB=microservices_db
MONGO_COLLECTION=operations
```

#### RabbitMQ
```
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
RABBITMQ_VHOST=/
```

#### Configuración adicional
```
DEBUG=true
```

Para Docker Compose, las variables de entorno se definen directamente en el archivo `docker-compose.yml`.

### Estructura del proyecto

```
TTelematica-RPC-P01/
├── api_gateway/
│   ├── app.py                     # Implementación del API Gateway
│   ├── Dockerfile.api-gateway     # Dockerfile para API Gateway
│   ├── operation_pb2.py           # Código generado por Protocol Buffers
│   └── operation_pb2_grpc.py      # Código generado por Protocol Buffers
├── client/
│   ├── client.py                  # Cliente REST
│   └── Dockerfile.client          # Dockerfile para el Cliente REST
├── common/
│   ├── __init__.py                # Hace que common sea un paquete importable
│   ├── db/
│   │   ├── __init__.py            # Hace que db sea un paquete importable
│   │   ├── config.py              # Configuración de la base de datos
│   │   └── operations_db.py       # Operaciones con MongoDB Atlas
│   └── mom/
│       ├── __init__.py            # Clase principal para MOM
│       ├── connection.py          # Gestión de conexiones a RabbitMQ
│       ├── operation_store.py     # Gestión del almacenamiento de operaciones
│       ├── queue_handler.py       # Manejo de colas de mensajes
│       └── recovery.py            # Recuperación de operaciones pendientes
├── microservices/
│   ├── protobufs/
│   │   ├── operation.proto        # Definición de servicios y mensajes
│   │   ├── operation_pb2.py       # Código generado por Protocol Buffers
│   │   └── operation_pb2_grpc.py  # Código generado por Protocol Buffers
│   ├── sum_service/
│   │   ├── Dockerfile.sum-service # Dockerfile para el servicio de suma
│   │   ├── operation_pb2.py       # Código generado por Protocol Buffers
│   │   ├── operation_pb2_grpc.py  # Código generado por Protocol Buffers
│   │   ├── server.py              # Servidor del microservicio de suma
│   │   └── service.py             # Implementación del servicio de suma
│   ├── substract_service/
│   │   ├── Dockerfile.substract-service # Dockerfile para el servicio de resta
│   │   ├── operation_pb2.py       # Código generado por Protocol Buffers
│   │   ├── operation_pb2_grpc.py  # Código generado por Protocol Buffers
│   │   ├── server.py              # Servidor del microservicio de resta
│   │   └── service.py             # Implementación del servicio de resta
│   └── mult_service/
│       ├── Dockerfile.mult-service # Dockerfile para el servicio de multiplicación
│       ├── operation_pb2.py       # Código generado por Protocol Buffers
│       ├── operation_pb2_grpc.py  # Código generado por Protocol Buffers
│       ├── server.py              # Servidor del microservicio de multiplicación
│       └── service.py             # Implementación del servicio de multiplicación
├── docker-compose.yml             # Configuración de Docker Compose
├── .env.example                   # Ejemplo de archivo de variables de entorno
├── requirements.txt               # Dependencias del proyecto
├── service_monitor.py             # Monitoreo de servicios
├── test_mongodb_connection.py     # Prueba de conexión a MongoDB
└── README.md                      # Documentación del proyecto
```

## 4. Descripción del ambiente de EJECUCIÓN (en producción)

### Ambiente de ejecución

- **Sistema Operativo**: Ubuntu Server 20.04 LTS o superior
- **Python 3.8+**
- **Docker y Docker Compose**
- **RabbitMQ**
- **MongoDB Atlas** (servicio en la nube)

### Configuración de las instancias EC2 en AWS

Para el despliegue en AWS EC2 se requieren dos instancias:

1. **api-gateway-instance**: Para alojar el API Gateway y RabbitMQ
2. **microservices-instance**: Para alojar los microservicios (suma, resta, multiplicación)

#### Creación y configuración de las instancias

1. **Crear instancia para api-gateway**:
   - Seleccionar Ubuntu Server 20.04 LTS
   - Configurar grupo de seguridad con las siguientes reglas de entrada:
     - SSH: Puerto 22
     - HTTP: Puerto 80
     - TCP personalizado: Puerto 5000 (API Gateway)
     - TCP personalizado: Puerto 5672 (RabbitMQ AMQP)
     - TCP personalizado: Puerto 15672 (RabbitMQ Management UI)
     - Todo el tráfico desde la IP privada de microservices-instance

2. **Crear instancia para microservices**:
   - Seleccionar Ubuntu Server 20.04 LTS
   - Configurar grupo de seguridad con las siguientes reglas de entrada:
     - SSH: Puerto 22
     - TCP personalizado: Puerto 50051 (Servicio de suma)
     - TCP personalizado: Puerto 50052 (Servicio de resta)
     - TCP personalizado: Puerto 50053 (Servicio de multiplicación)
     - Todo el tráfico desde la IP privada de api-gateway-instance

### Configuración para producción

Una vez creadas las instancias, se deben crear los archivos de configuración para Docker Compose en cada una de ellas:

1. **docker-compose.api-gateway.yml** (en la instancia api-gateway):
   ```yaml
   version: '3.8'
   
   services:
     # RabbitMQ (Message Broker)
     rabbitmq:
       image: rabbitmq:3-management
       ports:
         - "5672:5672"  # AMQP protocol
         - "15672:15672"  # Management UI
       environment:
         - RABBITMQ_DEFAULT_USER=guest
         - RABBITMQ_DEFAULT_PASS=guest
       volumes:
         - rabbitmq_data:/var/lib/rabbitmq
       healthcheck:
         test: rabbitmq-diagnostics -q ping
         interval: 10s
         timeout: 10s
         retries: 5
         start_period: 40s
       networks:
         - gateway_network
       restart: unless-stopped
   
     # API Gateway
     api-gateway:
       build:
         context: .
         dockerfile: api_gateway/Dockerfile.api-gateway
       ports:
         - "80:5000"  # Exponemos el puerto 80 para acceso web
       environment:
         - SUM_SERVICE_ADDRESS=IP_MICROSERVICES_INSTANCE:50051
         - SUBTRACT_SERVICE_ADDRESS=IP_MICROSERVICES_INSTANCE:50052
         - MULT_SERVICE_ADDRESS=IP_MICROSERVICES_INSTANCE:50053
         - MONGO_URI=mongodb+srv://admin:admin123@telematica-rpc.mxzpwyj.mongodb.net/microservices_db?retryWrites=true&w=majority
         - MONGO_DB=microservices_db
         - MONGO_COLLECTION=operations
         - RABBITMQ_HOST=rabbitmq
         - RABBITMQ_PORT=5672
         - RABBITMQ_USER=guest
         - RABBITMQ_PASS=guest
         - RABBITMQ_VHOST=/
         - DEBUG=true
       depends_on:
         rabbitmq:
           condition: service_healthy
       networks:
         - gateway_network
       restart: unless-stopped
   
     # Cliente
     client:
       build:
         context: .
         dockerfile: client/Dockerfile.client
       environment:
         - API_GATEWAY_URL=http://api-gateway:5000
       depends_on:
         - api-gateway
       networks:
         - gateway_network
       stdin_open: true
       tty: true
   
   networks:
     gateway_network:
       driver: bridge
   
   volumes:
     rabbitmq_data:
   ```

2. **docker-compose.microservices.yml** (en la instancia microservices):
   ```yaml
   version: '3.8'
   
   services:
     # Microservicio de Suma
     sum-service:
       build:
         context: .
         dockerfile: microservices/sum_service/Dockerfile.sum-service
       environment:
         - MONGO_URI=mongodb+srv://admin:admin123@telematica-rpc.mxzpwyj.mongodb.net/microservices_db?retryWrites=true&w=majority
         - MONGO_DB=microservices_db
         - MONGO_COLLECTION=operations
         - RABBITMQ_HOST=IP_API_GATEWAY_INSTANCE
         - RABBITMQ_PORT=5672
         - RABBITMQ_USER=guest
         - RABBITMQ_PASS=guest
         - RABBITMQ_VHOST=/
         - DEBUG=true
       ports:
         - "50051:50051"
       networks:
         - microservices_network
       restart: unless-stopped
       healthcheck:
         test: ["CMD", "python", "-c", "import socket; s=socket.socket(); s.connect(('localhost', 50051))"]
         interval: 30s
         timeout: 10s
         retries: 3
   
     # Microservicio de Resta
     subtract-service:
       build:
         context: .
         dockerfile: microservices/substract_service/Dockerfile.substract-service
       environment:
         - MONGO_URI=mongodb+srv://admin:admin123@telematica-rpc.mxzpwyj.mongodb.net/microservices_db?retryWrites=true&w=majority
         - MONGO_DB=microservices_db
         - MONGO_COLLECTION=operations
         - RABBITMQ_HOST=IP_API_GATEWAY_INSTANCE
         - RABBITMQ_PORT=5672
         - RABBITMQ_USER=guest
         - RABBITMQ_PASS=guest
         - RABBITMQ_VHOST=/
         - DEBUG=true
       ports:
         - "50052:50052"
       networks:
         - microservices_network
       restart: unless-stopped
       healthcheck:
         test: ["CMD", "python", "-c", "import socket; s=socket.socket(); s.connect(('localhost', 50052))"]
         interval: 30s
         timeout: 10s
         retries: 3
   
     # Microservicio de Multiplicación
     mult-service:
       build:
         context: .
         dockerfile: microservices/mult_service/Dockerfile.mult-service
       environment:
         - MONGO_URI=mongodb+srv://admin:admin123@telematica-rpc.mxzpwyj.mongodb.net/microservices_db?retryWrites=true&w=majority
         - MONGO_DB=microservices_db
         - MONGO_COLLECTION=operations
         - RABBITMQ_HOST=IP_API_GATEWAY_INSTANCE
         - RABBITMQ_PORT=5672
         - RABBITMQ_USER=guest
         - RABBITMQ_PASS=guest
         - RABBITMQ_VHOST=/
         - DEBUG=true
       ports:
         - "50053:50053"
       networks:
         - microservices_network
       restart: unless-stopped
       healthcheck:
         test: ["CMD", "python", "-c", "import socket; s=socket.socket(); s.connect(('localhost', 50053))"]
         interval: 30s
         timeout: 10s
         retries: 3
   
   networks:
     microservices_network:
       driver: bridge
   ```

**Importante:** Se deben reemplazar `IP_MICROSERVICES_INSTANCE` e `IP_API_GATEWAY_INSTANCE` con las IPs privadas reales de las instancias EC2.

### Instalación y despliegue

#### Configuración inicial de las instancias EC2

En ambas instancias (api-gateway y microservices), realizar los siguientes pasos:

1. Conectarse por SSH a la instancia.

2. Actualizar el sistema e instalar dependencias:
   ```bash
   sudo apt update
   sudo apt install docker.io -y
   sudo apt install docker-compose -y
   sudo systemctl enable docker
   sudo systemctl start docker
   ```

3. Verificar la instalación:
   ```bash
   docker --version
   docker-compose --version
   ```

4. Clonar el repositorio:
   ```bash
   git clone https://github.com/jdacunag/TTelematica-RPC-P01.git
   cd TTelematica-RPC-P01
   ```

5. Crear los archivos de configuración Docker Compose correspondientes:
   ```bash
   # En la instancia api-gateway:
   touch docker-compose.api-gateway.yml
   nano docker-compose.api-gateway.yml
   # Copiar el contenido de docker-compose.api-gateway.yml

   # En la instancia microservices:
   touch docker-compose.microservices.yml
   nano docker-compose.microservices.yml
   # Copiar el contenido de docker-compose.microservices.yml
   ```

#### Lanzamiento de los servicios

1. En la instancia api-gateway:
   ```bash
   sudo docker-compose -f docker-compose.api-gateway.yml build
   sudo docker-compose -f docker-compose.api-gateway.yml up -d
   ```

2. En la instancia microservices:
   ```bash
   sudo docker-compose -f docker-compose.microservices.yml build
   sudo docker-compose -f docker-compose.microservices.yml up -d
   ```

### Guía de usuario

#### Cliente interactivo (recomendado)

La forma más sencilla de probar el sistema es mediante el cliente interactivo proporcionado:

1. Ejecutar el cliente desde la instancia api-gateway:
   ```bash
   sudo docker-compose -f docker-compose.api-gateway.yml run --rm client
   ```

2. Se mostrará un menú interactivo con las siguientes opciones:
   ```
   === Cliente REST para Servicio Matemático ===

   Opciones:
   1. Verificar estado del servicio
   2. Realizar operación de suma
   3. Realizar operación de resta
   4. Realizar operación de multiplicación
   5. Consultar estado de operación
   6. Listar todas las operaciones
   7. Salir
   ```

3. Selecciona una opción siguiendo las instrucciones en pantalla. Por ejemplo:
   - Para realizar una suma, selecciona la opción 2 e ingresa los operandos cuando se te solicite.
   - Para consultar el estado de una operación, selecciona la opción 5 e ingresa el ID de la operación.

#### Cliente REST mediante curl

También se puede acceder a los servicios directamente mediante HTTP utilizando herramientas como curl:

1. Acceder al API Gateway desde el navegador o mediante herramientas como curl o Postman:
   ```
   http://[IP_PUBLICA_API_GATEWAY_INSTANCE]
   ```

2. Para ejecutar operaciones mediante la API:

   - **Suma**:
     ```bash
     curl -X POST http://[IP_PUBLICA_API_GATEWAY_INSTANCE]/sum \
       -H "Content-Type: application/json" \
       -d '{"a": 5, "b": 3}'
     ```

   - **Resta**:
     ```bash
     curl -X POST http://[IP_PUBLICA_API_GATEWAY_INSTANCE]/subtract \
       -H "Content-Type: application/json" \
       -d '{"a": 10, "b": 4}'
     ```

   - **Multiplicación**:
     ```bash
     curl -X POST http://[IP_PUBLICA_API_GATEWAY_INSTANCE]/mult \
       -H "Content-Type: application/json" \
       -d '{"a": 6, "b": 7}'
     ```

3. Consultar el estado de una operación:
   ```bash
   curl http://[IP_PUBLICA_API_GATEWAY_INSTANCE]/operation/status/[OPERATION_ID]
   ```

4. Ver todas las operaciones:
   ```bash
   curl http://[IP_PUBLICA_API_GATEWAY_INSTANCE]/operations
   ```

5. Verificar el estado de un servicio específico:
   ```bash
   curl http://[IP_PUBLICA_API_GATEWAY_INSTANCE]/service/status?service=sum
   ```

## 5. Información adicional relevante

### Simulación de fallos y prueba del mecanismo de failover

El sistema incluye comandos para simular fallos y probar el mecanismo de failover:

1. Acceder a la consola de un microservicio:
   ```bash
   docker exec -it [CONTAINER_ID] /bin/bash
   ```

2. En la consola del servidor, escribir `toggle` para cambiar el estado del servidor a degradado (simulando una falla).

3. Realizar una operación desde el cliente.

4. Observar cómo la operación se encola en RabbitMQ y el cliente recibe una notificación.

5. Escribir `toggle` nuevamente para restaurar el servidor y procesar las operaciones pendientes.

6. También se puede usar el comando `process` para procesar manualmente las operaciones pendientes.

### MongoDB Atlas

El sistema utiliza MongoDB Atlas como base de datos en la nube para almacenar las operaciones y su estado. Las colecciones están estructuradas de la siguiente manera:

- **Colección 'operations'**: Almacena todas las operaciones con los siguientes campos:
  - `operation_id`: Identificador único de la operación
  - `status`: Estado actual (PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED)
  - `message`: Mensaje descriptivo del estado
  - `result`: Resultado de la operación (si está completada)
  - `timestamp`: Marca de tiempo de la última actualización
  - `service`: Servicio que procesa la operación (sum, subtract, mult)
  - `a`: Primer operando
  - `b`: Segundo operando

## Referencias

- [gRPC Documentation](https://grpc.io/docs/)
- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [MongoDB Atlas Documentation](https://docs.atlas.mongodb.com/)
- [Docker Documentation](https://docs.docker.com/)
- [Protocol Buffers Documentation](https://developers.google.com/protocol-buffers)
- [Microsservices with Python and RabbitMQ](https://medium.com/better-programming/microservices-with-python-and-rabbitmq-f93d730c2e8a)
