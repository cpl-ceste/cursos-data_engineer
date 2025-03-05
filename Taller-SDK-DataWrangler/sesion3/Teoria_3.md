# AWS Data Wrangler – Sesión 3

## Índice
1. [Introducción](#introduccion)
2. [Amazon Athena](#athena)
   1. Modos de ejecución con AWS Data Wrangler
   2. Lectura con ctas_approach, unload_approach y CSV
   3. Uso de categorías (parámetro categories)
   4. Batching (carga por lotes)
   5. Queries parametrizadas
   6. Limpieza (wr.s3.delete_objects) y gestión del Glue Catalog
3. [Amazon EMR](#emr)
   1. Creación de un clúster EMR con Data Wrangler
   2. Subida de scripts PySpark a S3 y ejecución de Steps
   3. Integración con Docker (opcional)
4. [Amazon DynamoDB](#dynamodb)
   1. Escribir datos (DataFrame, CSV, JSON, lista de items)
   2. Leer datos (scan / queries)
   3. Uso de PartiQL (wr.dynamodb.execute_statement)
5. [Amazon S3 Select](#s3-select)
   1. Características y usos comunes
   2. Limitaciones
   3. Ejemplos de uso

6. [Recursos Adicionales](#recursos)

## 1. Introducción <a name="introduccion"></a>
En esta tercera sesión, profundizaremos en el uso de AWS Data Wrangler para interactuar con servicios como Amazon Athena, EMR, DynamoDB y S3 Select. Veremos cómo simplifica la lectura, escritura y manipulación de datos.

## 2. Amazon Athena <a name="athena"></a>
Amazon Athena es un servicio de consulta interactiva de Amazon Web Services que permite analizar datos directamente en Amazon S3 utilizando SQL estándar. Es un servicio serverless, lo que significa que no requiere aprovisionamiento de servidores ni gestión de infraestructura. Simplemente cargas los datos en S3, defines el esquema, y puedes ejecutar consultas utilizando la consola de Athena o APIs. Athena utiliza Presto como motor de consultas y es compatible con múltiples formatos como CSV, JSON, Parquet y ORC.

**Características y Usos Comunes:**
- Consultas ad hoc sobre grandes volúmenes de datos almacenados en S3.
- Integración con AWS Glue para la gestión del catálogo de datos.
- Ideal para casos de Business Intelligence (BI) y reporting.
- Facturación basada en los datos escaneados, lo que lo hace coste-efectivo para consultas esporádicas.

### 2.1. Modos de ejecución
### Formas de ejecutar consultas en Athena
AWS Data Wrangler ofrece tres métodos para ejecutar consultas en Athena y obtener los resultados como un DataFrame:

#### 1. ctas_approach=True (Por defecto)
Envuelve la consulta con un CTAS y luego lee los datos como parquet directamente desde S3.

**Pros:**
- Más rápido para tamaños de resultados medianos y grandes.
- Puede manejar algunos tipos anidados.

**Contras:**
- Requiere permisos para crear/eliminar tablas en Glue.
- No soporta timestamp con zona horaria.
- No soporta columnas con nombres repetidos.
- No soporta columnas con tipos de datos indefinidos.
- Crea una tabla temporal que luego se elimina inmediatamente.
- No soporta `data_source` o `catalog_id` personalizados.

#### 2. unload_approach=True y ctas_approach=False
Ejecuta una consulta UNLOAD en Athena y analiza los resultados en formato Parquet desde S3.

**Pros:**
- Más rápido para tamaños de resultados medianos y grandes.
- Puede manejar algunos tipos anidados.
- No modifica el Glue Data Catalog.

**Contras:**
- El path de salida en S3 debe estar vacío.
- No soporta timestamp con zona horaria.
- No soporta columnas con nombres repetidos.
- No soporta columnas con tipos de datos indefinidos.

#### 3. ctas_approach=False
Ejecuta una consulta regular en Athena y analiza los resultados en formato CSV desde S3.

**Pros:**
- Más rápido para tamaños pequeños (menor latencia).
- No requiere permisos para crear/eliminar tablas en Glue.
- Soporta timestamp con zona horaria.
- Soporta `data_source` y `catalog_id` personalizados.

**Contras:**
- Más lento (aunque sigue siendo más rápido que otras bibliotecas que utilizan la API regular de Athena).
- No maneja tipos anidados.


### Ejemplo de Lectura
```python
import awswrangler as wr
df = wr.athena.read_sql_query(sql="SELECT * FROM mi_tabla", database="mi_database")
```

## 3. Amazon EMR <a name="emr"></a>
Amazon EMR (Elastic MapReduce) es un servicio que permite procesar grandes cantidades de datos utilizando frameworks como Apache Hadoop y Apache Spark. EMR facilita la creación y gestión de clústeres para ejecutar tareas ETL, análisis de logs, modelado de datos y machine learning.

**Características y Usos Comunes:**
- Gestión automatizada del clúster (escalado, aprovisionamiento y monitoreo).
- Soporte para múltiples frameworks: Hadoop, Spark, HBase, Presto.
- Integración nativa con S3, DynamoDB y otros servicios AWS.
- Ideal para pipelines ETL, análisis de big data y tareas de machine learning.

### 3.1. Creación de Clúster
```python
import awswrangler as wr
cluster_id = wr.emr.create_cluster(subnet_id="subnet-123abc")
```

### 3.2. Ejecución de Steps
```python
step_id = wr.emr.submit_step(cluster_id, command="spark-submit s3://mi-bucket/demo.py")
```

## 4. Amazon DynamoDB <a name="dynamodb"></a>
Amazon DynamoDB es una base de datos NoSQL totalmente gestionada y distribuida, diseñada para aplicaciones que requieren rendimiento a escala y baja latencia. DynamoDB maneja automáticamente la replicación y el escalado horizontal sin intervención manual.

**Características y Usos Comunes:**
- Modelo de datos flexible basado en tablas y elementos (clave-valor y documentos).
- Bajo tiempo de respuesta en milisegundos para aplicaciones críticas.
- Soporte para transacciones ACID, Streams y backup/restauración.
- Ideal para aplicaciones web, móviles, IoT, gaming y comercio electrónico.

### 4.1. Escribir datos
```python
import pandas as pd
import awswrangler as wr
df = pd.DataFrame({"title": ["Titanic", "Snatch"], "year": [1997, 2000], "genre": ["drama", "crime"]})
wr.dynamodb.put_df(df=df, table_name="movies")
```
### 4.2. Escribir CSV archivos
```python
filepath = Path("items.csv")
df.to_csv(filepath, index=False)
wr.dynamodb.put_csv(path=filepath, table_name=table_name)
filepath.unlink()
```

### 4.3. Escribir JSON archivos
```python
filepath = Path("items.json")
df.to_json(filepath, orient="records")
wr.dynamodb.put_json(path="items.json", table_name=table_name)
filepath.unlink()
```

## 5. Amazon S3 Select <a name="s3-select"></a>
Amazon S3 Select es una característica de Amazon S3 que permite realizar consultas SQL directamente sobre los objetos almacenados en S3, como archivos CSV, JSON o Parquet, sin necesidad de descargar todo el archivo. Esto mejora significativamente el rendimiento (hasta un 400% en comparación con operaciones tradicionales como read_parquet) y reduce los costes al procesar solo los datos relevantes.

**Características y Usos Comunes:**
- Permite ejecutar consultas SQL sobre archivos grandes (incluso de varios TB).
- Soporta formatos comprimidos y anidados (solo en JSON).
- Reduce la latencia y los costes de procesamiento.
- Ideal para escenarios donde solo se requiere una parte específica de los datos almacenados en grandes archivos.

**Limitaciones:**
- Tamaño máximo de un registro en la entrada o resultado: 1 MB.
- Tamaño máximo descomprimido de un grupo de filas: 256 MB (solo Parquet).
- No soporta operaciones como ORDER BY.

### Ejemplo de uso:
```python
import awswrangler as wr

df = wr.s3.select_query(
    sql='SELECT * FROM s3object s where s."trip_distance" > 30',
    path="s3://ursa-labs-taxi-data/2019/01/",
    input_serialization="Parquet",
    input_serialization_params={},
)
df.head()
```

Otro ejemplo con CSV:
```python
df = wr.s3.select_query(
    sql="SELECT * FROM s3object",
    path="s3://humor-detection-pds/Humorous.csv",
    input_serialization="CSV",
    input_serialization_params={
        "FileHeaderInfo": "Use",
        "RecordDelimiter": "\r\n",
    },
    scan_range_chunk_size=1024 * 1024 * 32,
    use_threads=True,
)
df.head()
```

## 6. Recursos Adicionales <a name="recursos"></a>
- [Repositorio oficial AWS Data Wrangler](https://github.com/awslabs/aws-data-wrangler)
- [Documentación Amazon Athena](https://docs.aws.amazon.com/athena)
- [Documentación Amazon EMR](https://docs.aws.amazon.com/emr)
- [Documentación Amazon DynamoDB](https://docs.aws.amazon.com/dynamodb)
- [Documentación Amazon S3 Select](https://docs.aws.amazon.com/AmazonS3/latest/dev/selecting-content-from-objects.html)
