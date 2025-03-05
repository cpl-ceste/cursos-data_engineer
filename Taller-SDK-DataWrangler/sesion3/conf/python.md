### Ejercicio 2: Amazon EMR
**Enunciado:**
1. Crea un clúster EMR con múltiples nodos.
2. Sube varios scripts PySpark a S3.
3. Ejecuta steps en secuencia para procesar grandes volúmenes de datos.
4. Guarda los resultados en S3 en formato Parquet y CSV.
5. Realiza consultas usando AWS Data Wrangler para verificar los resultados.

**Solución en Python:**
```python
import awswrangler as wr

# Crear clúster EMR
cluster_id = wr.emr.create_cluster(
    subnet_id="subnet-123abc",
    num_core_nodes=2
)

# Ejecutar step
step_id = wr.emr.submit_step(
    cluster_id,
    command="spark-submit s3://my-bucket/script.py"
)
```

### Ejercicio 3: Amazon DynamoDB
**Enunciado:**
1. Crea una tabla llamada `products` con clave compuesta (`product_id` y `category`).
2. Inserta datos desde DataFrames y JSON.
3. Realiza consultas avanzadas utilizando `scan` y `query`.
4. Implementa operaciones transaccionales y actualizaciones utilizando PartiQL.
5. Genera informes agregados utilizando AWS Data Wrangler.

**Solución en Python:**
```python
import awswrangler as wr
import pandas as pd

df = pd.DataFrame({
    "product_id": ["123", "124"],
    "name": ["Laptop", "Mouse"],
    "category": ["Electronics", "Accessories"]
})

# Insertar datos
wr.dynamodb.put_df(df=df, table_name="products")
```

### Ejercicio 4: Amazon S3 Select
**Enunciado:**
1. Sube múltiples archivos CSV y Parquet a S3 con datos complejos.
2. Utiliza S3 Select para consultar diferentes columnas y aplicar filtros avanzados.
3. Combina los resultados y guarda un nuevo archivo optimizado en S3.
4. Implementa `scan_range_chunk_size` y paralelismo para mejorar el rendimiento.
5. Realiza informes utilizando consultas SQL avanzadas.

**Solución en Python:**
```python
import awswrangler as wr

# Consultar datos en S3 Select
df = wr.s3.select_query(
    sql='SELECT * FROM s3object WHERE s."age" > 30',
    path="s3://my-bucket/data.csv",
    input_serialization="CSV"
)
print(df.head())
```