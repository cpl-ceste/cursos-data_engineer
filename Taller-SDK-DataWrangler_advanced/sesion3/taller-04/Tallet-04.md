# 3.1. Descripción del Taller

**Orquestación de un pipeline de análisis de vuelos y satisfacción de clientes con AWS Step Functions**

### Objetivo

Construir una máquina de estado (*State Machine*) en **Step Functions** que invoque secuencialmente (o en paralelo, si se desea) nuestras funciones **Lambda** de sesiones anteriores. Al completarse con éxito, se habrán cargado los datos en **Amazon Redshift** y generado en **Amazon S3** el archivo final procesado. Si alguna fase falla, Step Functions lo registrará y detendrá el flujo (o podremos añadir un paso de compensación si lo necesitamos).

### Flujo General

1. **Paso 1**: Invocar la Lambda que procesa los datos de `flights.csv` y `passengers.csv`, y los inserta en la tabla `flights_analysis`. (Correspondiente a Taller 01 o 03 adaptado).  
2. **Paso 2**: Invocar la Lambda que calcula el retraso promedio y la satisfacción, escribiendo la salida en la tabla `flight_feedback_summary` y en S3. (Similar a Taller 02).  
3. **Paso 3**: Invocar la Lambda final que añade *timestamp* y copia a la tabla `flight_feedback_summary_lab`. (Taller 03).  
4. **Fin**: El flujo finaliza exitosamente.

---

# 4.Explicación código Lambdas

## 4.1. Lambda Paso 1 (`fn-training-activity-01`)

**Descripción**:  
Esta función **lee** los archivos `flights.csv` y `passengers.csv` desde S3, realiza un *merge* para calcular el número de pasajeros por vuelo (`passenger_count`) y el porcentaje de ocupación (`occupancy_rate`). Finalmente, **inserta** esos datos en la tabla `flights_analysis` de Amazon Redshift.

## 4.2. Lambda Paso 2 (`fn-training-activity-02`)

**Descripción**:  
Esta función lee los archivos `flights.csv` y `feedback.csv` para calcular el retraso promedio y la calificación promedio de cada vuelo. Posteriormente, inserta los resultados en la tabla `flight_feedback_summary` de Amazon Redshift y guarda un CSV final en S3 (ruta staging/flight_feedback_summary_lab.csv).

## 4.3. Lambda Paso 3 (`fn-training-activity-03`)

**Descripción**:  
Esta función lee el archivo `flight_feedback_summary_lab.csv `(generado en el paso anterior) y le añade un campo de timestamp (date_insert) a cada registro. Luego, carga esos datos en la tabla `flight_feedback_summary_lab` de Amazon Redshift.


## Crear tablas Redshift:
 
```sql
CREATE TABLE flights_analysis (
    flight_number VARCHAR(10),
    departure_city VARCHAR(50),
    arrival_city VARCHAR(50),
    capacity INT,
    passenger_count INT,
    occupancy_rate FLOAT
  
);
```

```sql
CREATE TABLE flight_feedback_summary_lab (
    flight_number VARCHAR(10),
    origin VARCHAR(50),
    destination VARCHAR(50),
    average_delay FLOAT,
    average_rating FLOAT,
    date_insert TIMESTAMP
);
```

### Conclusiones y Próximos Pasos

1. **Automatizacion**: Con Step Functions, ya no necesitamos lanzar manualmente cada Lambda en secuencia, sino que el flujo se coordina automáticamente.
2. **Escalabilidad**: Cada Lambda escala individualmente; Step Functions coordina las dependencias.
3. **Manejo de errores**: A través de Catch y Retry, simplificamos la lógica de reintentos.
4. **Orquestación avanzada**: Podríamos añadir bifurcaciones (Choice), paralelismo (Parallel) e integraciones con otro servicios aws tipo Glue, SageMaker, etc.