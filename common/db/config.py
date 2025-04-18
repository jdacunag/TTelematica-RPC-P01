import os
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env si existe
load_dotenv()

# Configuración MongoDB Atlas
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb+srv://usuario:password@cluster.mongodb.net/microservices_db?retryWrites=true&w=majority')
MONGO_DB = os.environ.get('MONGO_DB', 'microservices_db')
MONGO_COLLECTION = os.environ.get('MONGO_COLLECTION', 'operations')

# Otras configuraciones
DEBUG = os.environ.get('DEBUG', 'False').lower() in ('true', '1', 't')