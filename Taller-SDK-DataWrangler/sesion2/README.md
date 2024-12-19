

# Taller: Procesamiento y Análisis de Datos para una Aerolínea


## Consigna para el Taller de Alumnos
**Título:** Procesamiento y Análisis de Datos para una Aerolínea
**Descripción:**
Imagina que trabajas en el área de análisis de datos de una aerolínea. Se te asigna la tarea de
integrar datos operativos de vuelos y datos de pasajeros para generar un informe sobre el nivel de
ocupación de los vuelos. Este informe se utilizará para optimizar la asignación de recursos y
mejorar las operaciones de la aerolínea.

**Tareas:**

1. **Obtención de Datos:** La aerolínea proporciona dos archivos CSV almacenados en un bucket
de S3:
- `flights.csv`: Información sobre vuelos programados.
- `passengers.csv`: Información sobre pasajeros que han reservado boletos.

**Archivo `flights.csv`:**
```
flight_number,origin,destination,delay_minutes
flight_number,origin,destination,date,capacity
AA101,NYC,LAX,2024-12-01,200
AA102,LAX,CHI,2024-12-01,180
AA103,NYC,CHI,2024-12-02,150
AA104,LAX,NYC,2024-12-02,120
```
**Archivo `passengers.csv`:**
```
passenger_id,flight_number,name,nationality,fare
1,AA101,John Doe,USA,300
2,AA101,Jane Smith,UK,350
3,AA102,Mike Brown,USA,250
4,AA103,Alice Johnson,CAN,400
5,AA103,David Wilson,AUS,420
6,AA104,Emma Davis,USA,180
```

2. **Procesamiento de Datos:**
- Combina los dos archivos utilizando el número de vuelo (`flight_number`) como clave.
- Calcula la cantidad de pasajeros por vuelo (`passenger_count`).
- Calcula el porcentaje de ocupación de cada vuelo (`occupancy_rate`) utilizando la fórmula:
occupancy_rate = (passenger_count / capacity) * 100
3. **Cargar los Resultados en Redshift:**
Página 1
Taller: Procesamiento y Análisis de Datos para una Aerolínea
- Almacena los datos procesados en una tabla llamada `flight_summary` en Amazon Redshift.
**Resultados Esperados:**
Los estudiantes deberán implementar la Lambda, probarla con los datos proporcionados y
confirmar que los resultados se carguen correctamente en Redshift.


## Consigna para el Taller de Alumnos
Eres parte del equipo de análisis de datos de una aerolínea que quiere identificar cómo los retrasos
en los vuelos afectan la satisfacción de los pasajeros. Dispones de dos conjuntos de datos clave:
1. **`flights.csv`:** Contiene información sobre los vuelos realizados, como el número de vuelo,
origen, destino, y retrasos en minutos.
2. **`feedback.csv`:** Contiene opiniones de los pasajeros sobre su experiencia, incluyendo una
calificación de 1 a 5.
**Objetivo:**
Combinar estos datos y generar un informe que muestre el retraso promedio por vuelo y su impacto
en la calificación promedio de los pasajeros. Este informe se almacenará en una base de datos
**Redshift**.
---
**Datos de Entrada**
Página 1
Taller: Procesamiento y Análisis de Datos para una Aerolínea
**Archivo `flights.csv`:**
```
flight_number,origin,destination,delay_minutes
AA101,NYC,LAX,30
AA102,LAX,CHI,0
AA103,NYC,CHI,45
AA104,LAX,NYC,10
```
**Archivo `feedback.csv`:**
```
feedback_id,flight_number,customer_id,rating
1,AA101,C001,4
2,AA101,C002,3
3,AA102,C003,5
4,AA103,C004,2
5,AA103,C005,3
6,AA104,C006,5
```
---
**Tareas:**
Página 2
Taller: Procesamiento y Análisis de Datos para una Aerolínea
1. **Combinar Archivos:**
- Combina los datos de los vuelos (`flights.csv`) y las opiniones de los clientes (`feedback.csv`)
utilizando el número de vuelo (`flight_number`) como clave.
2. **Calcular Métricas:**
- Calcula el retraso promedio por vuelo.
- Calcula la calificación promedio por vuelo.
3. **Almacenar Resultados en Redshift:**
- Almacena las métricas en una tabla llamada `flight_feedback_summary`.
---
**Estructura de la Tabla en Redshift**
```sql
CREATE TABLE public.flight_feedback_summary (
flight_number VARCHAR(10),
origin VARCHAR(10),
destination VARCHAR(10),
average_delay FLOAT,
average_rating FLOAT
);
```

