## 6. Ejercicios Propuestos <a name="ejercicios"></a>
### Ejercicio 1: Amazon Athena
**Enunciado:**
1. Crea una base de datos llamada `athena_demo` en Glue.
2. Sube varios archivos CSV a S3 y crea tablas externas en `athena_demo`.
3. Realiza consultas utilizando `ctas_approach` y `unload_approach`.
4. Implementa categorías y carga por lotes.
5. Genera informes agregados utilizando funciones SQL como `GROUP BY` y `ORDER BY`.

### Ejercicio 2: Amazon EMR
**Enunciado:**
1. Crea un clúster EMR con múltiples nodos.
2. Sube varios scripts PySpark a S3.
3. Ejecuta steps en secuencia para procesar grandes volúmenes de datos.
4. Guarda los resultados en S3 en formato Parquet y CSV.
5. Realiza consultas usando AWS Data Wrangler para verificar los resultados.

### Ejercicio 3: Amazon DynamoDB
**Enunciado:**
1. Crea una tabla llamada `products` con clave compuesta (`product_id` y `category`).
2. Inserta datos desde DataFrames y JSON.
3. Realiza consultas avanzadas utilizando `scan` y `query`.
4. Implementa operaciones transaccionales y actualizaciones utilizando PartiQL.
5. Genera informes agregados utilizando AWS Data Wrangler.

### Ejercicio 4: Amazon S3 Select
**Enunciado:**
1. Sube múltiples archivos CSV y Parquet a S3 con datos complejos.
2. Utiliza S3 Select para consultar diferentes columnas y aplicar filtros avanzados.
3. Combina los resultados y guarda un nuevo archivo optimizado en S3.
4. Implementa `scan_range_chunk_size` y paralelismo para mejorar el rendimiento.
5. Realiza informes utilizando consultas SQL avanzadas.

## 6. Requisitos Previos y Configuración <a name="requisitos-previos"></a>
Para resolver los ejercicios propuestos, asegúrate de cumplir con los siguientes requisitos y configuraciones:

### 6.1. Configuración de AWS CLI
1. Instalar AWS CLI:
```bash
pip install awscli
```

2. Configurar las credenciales de AWS:
```bash
aws configure
```
Se te pedirá ingresar:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name (ej: us-east-1)
- Default output format (ej: json)

### 6.2. Crear un bucket S3 y subir archivos
**Crear un bucket:**
```bash
aws s3api create-bucket --bucket my-bucket --region us-east-1
```

**Subir archivos CSV a S3:**
```bash
aws s3 cp data.csv s3://my-bucket/
```

### 6.3. Configurar AWS Glue para Amazon Athena
**Crear un catálogo de datos en Glue:**
```bash
aws glue create-database --database-input '{"Name":"athena_demo"}'
```

**Crear una tabla en Glue:**
```bash
aws glue create-table --database-name athena_demo --table-input file://table-definition.json
```
Donde `table-definition.json` es un archivo JSON con la estructura de la tabla.

### 6.4. Permisos necesarios
Asegúrate de que el usuario o rol tenga las políticas necesarias:
- `LabRole`
- `AmazonS3ReadOnlyAccess`
- `AmazonAthenaFullAccess`
- `AWSGlueConsoleFullAccess`
- `AmazonDynamoDBFullAccess`

Puedes adjuntar políticas con:
```bash
aws iam attach-user-policy --user-name my-user --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### 6.5. Configuración de EMR
**Crear un clúster EMR:**
```bash
aws emr create-cluster --name "EMR Cluster" --release-label emr-6.4.0 --applications Name=Hadoop,Spark --ec2-attributes KeyName=my-key --instance-type m5.xlarge --instance-count 3
```

**Subir scripts a S3:**
```bash
aws s3 cp script.py s3://my-bucket/
```

**Ejecutar un Step:**
```bash
aws emr add-steps --cluster-id j-XXXXXXXX --steps Type=Spark,Name="Spark Step",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://my-bucket/script.py]
```

### 6.6. Configuración de DynamoDB
**Crear una tabla:**
```bash
aws dynamodb create-table --table-name products --attribute-definitions AttributeName=product_id,AttributeType=S --key-schema AttributeName=product_id,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

**Insertar datos en DynamoDB:**
```bash
aws dynamodb put-item --table-name products --item '{"product_id": {"S": "123"}, "name": {"S": "Laptop"}}'
```

### 6.7. Instalación de AWS Data Wrangler
```bash
pip install awswrangler
```

### 6.8. Configuración de IAM y Roles
Crea un rol con permisos para S3, Glue, Athena y DynamoDB. Asegúrate de adjuntar el rol a los servicios correspondientes.

## 7. Comandos AWS CLI <a name="aws-cli"></a>
### 7.1. Crear un bucket S3
```bash
aws s3api create-bucket --bucket my-bucket --region us-east-1
```

### 7.2. Subir un archivo CSV a S3
```bash
aws s3 cp data.csv s3://my-bucket/
```

### 7.3. Amazon Athena
```bash
aws athena start-query-execution --query-string "CREATE DATABASE athena_demo;" --result-configuration OutputLocation=s3://my-bucket/results/
```

### 7.4. Amazon EMR
```bash
aws emr create-cluster --name "EMR Cluster" --release-label emr-6.4.0 --applications Name=Hadoop,Spark --ec2-attributes KeyName=my-key --instance-type m5.xlarge --instance-count 3
```

### 7.5. Amazon DynamoDB
```bash
aws dynamodb create-table --table-name movies --attribute-definitions AttributeName=title,AttributeType=S --key-schema AttributeName=title,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

### 7.6. Amazon S3 Select
```bash
aws s3api select-object-content --bucket my-bucket --key data.csv --expression "SELECT * FROM s3object s WHERE s.age > 30" --expression-type SQL --input-serialization '{"CSV": {"FileHeaderInfo": "Use"}}' --output-serialization '{"CSV": {}}'
```