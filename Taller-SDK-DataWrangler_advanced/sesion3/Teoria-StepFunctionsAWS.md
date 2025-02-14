# Sesión 4: Orquestación de Pipelines de Datos con AWS Step Functions

## 1. Objetivos de la Sesión

1. **Reforzar** los conceptos aprendidos en sesiones anteriores:
   - Lectura y escritura de datos en **Amazon S3**.
   - Procesamiento de datos con **pandas** y **awswrangler**.
   - Inserción de datos en **Amazon Redshift** usando Lambdas.
   - Uso de **CloudFormation** para aprovisionar la infraestructura.

2. **Nuevo servicio**  de orquestación llamado **AWS Step Functions**, que nos permite coordinar múltiples servicios de AWS y pasos lógicos de forma visual y serverless.

3. **Construir** un flujo de trabajo (*pipeline*) con varios **Lambda** que se ejecuta de manera secuencial, con manejo básico de errores y notificaciones.

4. **Automatizar** el pipeline utilizando un evento programado  o dejarlo listo para ser invocado manualmente desde **Step Functions**.

5. **Taller práctico** para consolidar la experiencia práctica que integre todos estos elementos.


## 2. Conceptos teóricos

### 2.1. Repaso sesiones anteriores

- **AWS Lambda**  
  - Cómo creamos funciones para procesar datos de S3.  
  - Activación por eventos S3 (`s3:ObjectCreated:*`).  
  - Inserción en Amazon Redshift usando conectores temporales `awswrangler.redshift.copy`.

- **AWS Glue** (concepto y diferencias con Lambda)  
  - Cuándo conviene usar Lambda (procesamientos rápidos, <15min).  
  - Cuándo conviene usar Glue (ETL pesado, Spark, grandes volúmenes de datos).

- **Amazon Redshift**  
  - Almacén de datos (*data warehouse*) para análisis.  
  - Ingesta de tablas con `wr.redshift.copy`.  


- **AWS SDK for pandas (awswrangler)**  
  - Lectura y escritura de DataFrames en S3 (`wr.s3.read_csv`, `wr.s3.to_csv`).  
  - Conexión a Redshift (`wr.redshift.copy`, `wr.redshift.connect_temp`).

### 2.2. Nuevo Servicio ¿Qué es AWS Step Functions?

**AWS Step Functions** es un servicio orquestador de flujos de trabajo (*Workflows*) que te permite coordinar varios servicios de AWS en secuencias lógicas llamadas **máquinas de estado** (*State Machines*).

- **State Machine**: Diagrama donde cada **estado** (*state*) representa una tarea o paso a ejecutar.  
- **Task State**: Paso que invoca un servicio de AWS (p. ej., una función Lambda).  
- **Choice State**: Permite tomar decisiones condicionales dentro del flujo.  
- **Parallel State** (opcional, para flujos avanzados): Ejecuta varias ramas en paralelo.  
- **Retry & Catch**: Manejo de errores y reintentos automáticos.  
- **Visual Workflow**: Interfaz gráfica en AWS para entender la secuencia y estado del flujo.

#### Casos de uso de Step Functions

- Orquestar múltiples Lambdas de procesamiento en un orden específico.  
- Manejar errores y reintentos sin necesidad de programar esa lógica en cada Lambda.  
- Ejecutar *pipelines* de datos, *machine learning* o flujos de operaciones repetitivas de forma centralizada.

### 2.3. Arquitectura que Construiremos

1. **S3**: Almacenará archivos de vuelos, *feedback* o similares (igual que en laboratorios anteriores).  
2. **Redshift**: Base de datos analítica donde iremos cargando los resultados.  
3. **Lambdas**: Varias funciones que:  
   - Limpian/combinan datos,  
   - Calculan métricas,  
   - Insertan en Redshift,  
   - Guardan archivos de salida en S3.  
4. **Step Functions**:
   - Orquestará la llamada secuencial a cada Lambda,  
   - Revisará si hay error,  
   - Continuará con el siguiente paso,  
   -
Al finalizar, tendremos un **pipeline orquestado paso a paso**, evitando ejecuciones manuales y facilitando la **automatización** de los procesos de ETL/ELT.
