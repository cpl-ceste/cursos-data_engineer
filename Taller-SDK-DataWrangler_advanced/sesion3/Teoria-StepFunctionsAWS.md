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
Con AWS Step Functions, puedes crear flujos de trabajo (workflows) para construir aplicaciones distribuidas, automatizar procesos, orquestar microservicios y crear pipelines de datos y machine learning.

En Step Functions, los flujos de trabajo son series de pasos controlados por eventos. Cada paso se denomina estado. Por ejemplo, un estado de tipo Task representa una unidad de trabajo que realiza otro servicio de AWS, como llamar a una API o servicio. Las instancias de flujos de trabajo en ejecución se llaman ejecuciones (executions).

En la consola de Step Functions, puedes visualizar, editar y depurar visualmente el flujo de trabajo de tu aplicación. Puedes examinar el estado de cada paso para asegurarte de que tu aplicación se ejecuta en orden y según lo esperado.

#### Tipos de flujos de trabajo: Standard y Express

Step Functions ofrece dos tipos de flujos de trabajo:

**Flujos de trabajo Standard**:
- Ideales para procesos de larga duración que requieren auditoría.
- Muestran historial de ejecución completo y depuración visual.
- Garantizan ejecución exactamente una vez (cada paso se ejecuta exactamente una vez).
- Pueden ejecutarse hasta por un año.
- Capacidad de 2,000 ejecuciones por segundo.
- Facturación basada en transiciones de estado.

**Flujos de trabajo Express**:
- Ideales para cargas de trabajo de alta frecuencia como procesamiento de datos en streaming.
- Ejecución al menos una vez (un paso puede ejecutarse más de una vez).
- Duración máxima de 5 minutos.
- Capacidad de hasta 100,000 ejecuciones por segundo.
- Transiciones de estado prácticamente ilimitadas.
- Facturación basada en número y duración de ejecuciones.
- Historial de ejecución enviado a CloudWatch.

#### Patrones de integración con servicios

Step Functions ofrece tres patrones para integrarse con otros servicios de AWS:

1. **Request Response** (predeterminado):
   - Llama a un servicio y continúa al siguiente estado después de recibir una respuesta HTTP.
   - Compatible con flujos Standard y Express.

2. **Run a Job** (.sync):
   - Llama a un servicio y Step Functions espera a que el trabajo se complete.
   - Solo compatible con flujos Standard para servicios específicos.

3. **Wait for Callback** (.waitForTaskToken):
   - Llama a un servicio con un token de tarea y espera hasta que el token regrese con una devolución de llamada.
   - Solo compatible con flujos Standard para servicios específicos.

#### Casos de uso de Step Functions

- Orquestar múltiples Lambdas de procesamiento en un orden específico.  
- Manejar errores y reintentos sin necesidad de programar esa lógica en cada Lambda.  
- Ejecutar *pipelines* de datos, *machine learning* o flujos de operaciones repetitivas de forma centralizada.
- Orquestar tareas secuenciales donde cada paso proporciona entradas para el siguiente.
- Tomar decisiones basadas en datos usando estados Choice.
- Implementar manejo de errores sofisticado con Retry y Catch.
- Incluir aprobación humana en el flujo (human-in-the-loop).
- Procesar datos en pasos paralelos para mejorar el rendimiento.
- Procesar dinámicamente elementos de datos con estados Map.


### 2.3. Primer ejercicio: Crear una maquina de estado.

En este ejercicio se descargará, compilará e implementará una AWS SAM aplicación de ejemplo que contiene una máquina de AWS Step Functions estados. Esta aplicación crea un flujo de trabajo bursátil simulado que se ejecuta en un horario predefinido.

Enunciado: [Crear una maquina de estado](https://docs.aws.amazon.com/es_es/step-functions/latest/dg/tutorial-stock-trading-workflow.html)

### 2.4. Arquitectura que construiremos en el 2º ejercicio.

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


