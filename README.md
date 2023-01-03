# agroanalysis
Repositorio para el trabajo de la asignatura de minería de datos

El proyecto se distribuye en los 4 directorios principales, uno por cada fase:
    - extraction
    - load
    - serve
    - transform

Dentro de extraction, load y serve se encuentran:
    - requirements.txt: para instalar los componentes necesarios para cada parte
    - docker-compose.yml: para construir el contenedor docker
    - Dockerfile: para construir el contenedor docker
    - app (Directorio): directorio "raíz" del módulo

Dentro de cada directorio app se encuentra el directorio src, con el código
del módulo:
    - main.py: contiene los endpoint de FastAPI
    - extraction/load/serve .py: código principal del módulo
    - config.py: con la configuración de la conexión a la DB
    - database.ini: con los datos de conexión a la DB

Dentro de extraction y load existe el directorio data, con los ficheros
adicionales que sean necesarios:
    - extraction:
        - raw_data: con los subdirectorios que alojan los ficheros extraídos de
                    de las fuentes de datos
    - load:
        - data: con un fichero .csv que contiene un diccionario para sustutuir
                nombres de productos en los datastes

Dentro de serve existe el directorio templates, el cual alojará las plantillas
html donde se visualizarán los datos extraídos de la base de datos.

---

Para desplegar el sistema, basta con tener instalado python3.10 (o mayor),
docker y docker compose en el equipo.

Cumplido estos requisitos, para desplegar cada módulo es necesario acceder
a su directorio, y ejecutar el comando:

docker compose up -d --build
    o
docker compose up --build

En caso de querer ejecutar en background el módulo, usar el primer comando.
De lo contrario, usar el segundo.

Los módulos están conectados entre sí en una red de docker, por lo que es
importante que todas estén en la misma red.

El módulo transform (base de datos en Postgresql) utiliza la DB llamada agro.
