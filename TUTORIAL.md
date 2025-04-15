# Tutorial Completo: DBT con DuckDB para Análisis de Datos

Este tutorial te guiará paso a paso en el uso del proyecto `dbt-duckdb-analytics-poc`, un ejemplo de cómo implementar DBT (Data Build Tool) con DuckDB para análisis de datos locales.

## 1. Introducción

### ¿Qué es DBT?

DBT (Data Build Tool) es una herramienta de transformación de datos que permite a los analistas y científicos de datos transformar datos de manera eficiente utilizando SQL. DBT se encarga de la orquestación, documentación y pruebas, permitiéndote concentrarte en escribir transformaciones.

### ¿Qué es DuckDB?

DuckDB es una base de datos analítica embebida, diseñada para ser rápida, eficiente y fácil de usar. Es particularmente buena para análisis de datos y consultas OLAP (Online Analytical Processing), y puede funcionar como un archivo independiente similar a SQLite.

### ¿Por qué utilizar DBT con DuckDB?

La combinación de DBT con DuckDB ofrece:
- Un flujo de trabajo de datos completamente local y portátil
- Transformaciones eficientes y rápidas
- Facilidad para modelar y transformar datos
- Capacidad de trabajar sin conexión
- Estructura modular y testeable para el código SQL

## 2. Requisitos Previos

Antes de comenzar, asegúrate de tener instalado:

- Python 3.8 o superior
- pip (gestor de paquetes de Python)
- Git

## 3. Instalación y Configuración

### 3.1. Clonar el Repositorio

```bash
git clone https://github.com/tu-usuario/dbt-duckdb-analytics-poc.git
cd dbt-duckdb-analytics-poc
```

### 3.2. Crear y Activar un Entorno Virtual

```bash
# En macOS/Linux
python -m venv venv
source venv/bin/activate

# En Windows
python -m venv venv
venv\Scripts\activate
```

### 3.3. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 3.4. Configurar el Perfil de DBT

DBT necesita saber cómo conectarse a DuckDB. El perfil ya está configurado en el archivo `profiles.yml`, pero debes asegurarte de que esté en el lugar correcto:

```bash
# Crear directorio .dbt en tu directorio home si no existe
mkdir -p ~/.dbt

# Copiar el archivo profiles.yml
cp profiles.yml ~/.dbt/
```

## 4. Estructura del Proyecto

El proyecto sigue una arquitectura moderna de datos con las siguientes capas:

### 4.1. Seeds (Semillas)

Los archivos CSV en la carpeta `seeds/` se utilizan como datos de entrada iniciales.

### 4.2. Staging Models

Los modelos en `models/staging/` realizan la limpieza y normalización inicial de los datos sin agregar transformaciones complejas.

### 4.3. Marts Models

Los modelos en `models/marts/` combinan datos de staging para crear dimensiones y tablas de hechos útiles para casos de uso específicos.

### 4.4. Core Models

Los modelos en `models/core/` representan métricas y entidades de negocio altamente transformadas.

### 4.5. Otros Componentes

- `analyses/`: Consultas SQL exploratorias que no forman parte del modelo formal
- `macros/`: Fragmentos de código SQL reutilizables
- `tests/`: Pruebas para verificar la calidad e integridad de los datos
- `snapshots/`: Capturas de datos históricos para seguimiento de cambios

## 5. Flujo de Trabajo Básico

### 5.1. Inicializar los Datos con Seeds

Los seeds son archivos CSV que se cargan directamente en la base de datos:

```bash
dbt seed
```

Este comando carga los archivos CSV de la carpeta `seeds/` en DuckDB.

### 5.2. Ejecutar los Modelos

Para ejecutar todos los modelos y construir tu data warehouse:

```bash
dbt run
```

Para ejecutar un modelo específico:

```bash
dbt run --select dim_customers
```

Para ejecutar modelos en una capa específica:

```bash
dbt run --select staging.*
```

### 5.3. Ejecutar Pruebas

Las pruebas garantizan la calidad e integridad de tus datos:

```bash
# Ejecutar todas las pruebas
dbt test

# Ejecutar pruebas para un modelo específico
dbt test --select dim_customers
```

### 5.4. Generar y Ver Documentación

DBT puede generar documentación detallada para tu proyecto:

```bash
# Generar documentación
dbt docs generate

# Servir documentación en un servidor web local
dbt docs serve
```

La documentación incluye:
- Lineage (dependencias entre modelos)
- Definiciones de columnas
- Pruebas asociadas a cada modelo
- Código SQL subyacente

## 6. Casos de Uso Prácticos

### 6.1. Análisis de Clientes

Explora el comportamiento de los clientes y su segmentación:

```bash
# Primero compilar los análisis
dbt compile

# Ejecutar el análisis de clientes
python run_analysis.py customer_analysis
```

O para un análisis más detallado con patrones de compra:

```bash
python run_analysis.py customer_orders_analysis
```

### 6.2. Rendimiento de Productos

Analiza qué productos tienen mejor rendimiento:

```bash
python run_analysis.py product_performance
```

### 6.3. Tendencias de Ingresos

Examina las tendencias de ingresos a lo largo del tiempo:

```bash
python run_analysis.py order_trends_by_time
```

También puedes guardar los resultados en un archivo CSV:

```bash
python run_analysis.py revenue_trends --save
```

## 7. Trabajando con Entornos Múltiples

El proyecto está configurado para soportar múltiples entornos:

### 7.1. Desarrollo (Dev)

```bash
dbt run --target dev
```

### 7.2. Staging

```bash
dbt run --target staging
```

### 7.3. Producción (Prod)

```bash
dbt run --target prod
```

Cada entorno utiliza un archivo DuckDB diferente para aislar los datos.

## 8. Capturas de Datos Históricos (Snapshots)

Los snapshots capturan cambios en los datos a lo largo del tiempo:

```bash
# Ejecutar todos los snapshots
dbt snapshot
```

Esto creará tablas en el esquema `snapshots` que capturan el historial de cambios con columnas adicionales:
- `dbt_valid_from`: Fecha/hora desde cuando el registro es válido
- `dbt_valid_to`: Fecha/hora hasta cuando el registro es válido (NULL si es la versión actual)
- `dbt_updated_at`: Marca de tiempo usada para el seguimiento
- `dbt_scd_id`: Identificador único para cada versión del registro

### 8.1. Consultar snapshots

Para ver los datos históricos, puedes usar:

```bash
# Consultar todos los registros de un snapshot
python run_query.py "SELECT * FROM snapshots.customers_snapshot ORDER BY customer_id, dbt_valid_from"

# Consultar solo registros actuales (versiones vigentes)
python run_query.py "SELECT * FROM snapshots.customers_snapshot WHERE dbt_valid_to IS NULL"

# Ver el historial de cambios de un registro específico
python run_query.py "SELECT * FROM snapshots.customers_snapshot WHERE customer_id = 1 ORDER BY dbt_valid_from"
```

### 8.2. Nota sobre DuckDB y snapshots

En DuckDB, asegúrate de usar `current_timestamp` (sin paréntesis) en lugar de `current_timestamp()` en tus archivos de snapshot para evitar errores:

## 9. Técnicas Avanzadas

### 9.1. Creación de Macros Personalizadas

Las macros permiten reutilizar código SQL. Ejemplo:

```sql
-- macros/format_date.sql
{% macro format_date(date_column) %}
    TO_CHAR({{ date_column }}, 'YYYY-MM-DD')
{% endmacro %}
```

Uso:
```sql
SELECT {{ format_date('order_date') }} AS formatted_date
FROM {{ ref('fct_orders') }}
```

### 9.2. Uso de Jinja y Templating

DBT utiliza el motor de plantillas Jinja2 para hacer el SQL más dinámico:

```sql
{% for category in ['Electronics', 'Clothing', 'Home'] %}
    SELECT 
        '{{ category }}' as category,
        SUM(total_revenue) as revenue
    FROM {{ ref('product_performance') }}
    WHERE product_category = '{{ category }}'
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
```

### 9.3. Materialización Incremental

Para tablas grandes que crecen con el tiempo, puedes usar materialización incremental:

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT 
    *
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

## 10. Depuración y Solución de Problemas

### 10.1. Compilación sin Ejecución

Para ver el SQL generado sin ejecutarlo:

```bash
dbt compile --select my_model
```

El SQL compilado estará en `target/compiled/{proyecto}/{modelo}.sql`.

### 10.2. Depuración con --vars

Puedes pasar variables a tu proyecto:

```bash
dbt run --vars '{"min_date": "2023-01-01", "max_date": "2023-12-31"}'
```

Y usarlas en tu SQL:

```sql
SELECT * FROM {{ ref('stg_orders') }}
WHERE order_date BETWEEN '{{ var("min_date") }}' AND '{{ var("max_date") }}'
```

### 10.3. Logs de DBT

Los logs de DBT se encuentran en:

```
logs/dbt.log
```

## 11. Extensiones y Personalización

### 11.1. Integración con Python

DuckDB se integra bien con Python a través de su cliente nativo. Puedes usar el script `run_analysis.py` que hemos creado o escribir tu propio código:

```python
import duckdb
import pandas as pd

# Conectar a la base de datos
conn = duckdb.connect('data/analytics_dev.duckdb')

# Consultar datos
df = conn.execute("SELECT * FROM main.core_business_metrics").fetchdf()

# Análisis con pandas
print(df.describe())

# Visualización con matplotlib
import matplotlib.pyplot as plt
df.plot(x='metric_name', y='metric_value', kind='bar')
plt.show()
```

### 11.2. Exportación de Datos

Puedes exportar datos fácilmente:

```bash
# Exportar a CSV
duckdb data/analytics_dev.duckdb -c "COPY (SELECT * FROM analytics.dim_customers) TO 'exports/customers.csv' (HEADER, DELIMITER ',')"

# Exportar a Parquet
duckdb data/analytics_dev.duckdb -c "COPY (SELECT * FROM analytics.fct_orders) TO 'exports/orders.parquet' (FORMAT PARQUET)"
```

## 12. Mejores Prácticas

### 12.1. Nomenclatura

Sigue estas convenciones:
- Modelos staging: `stg_[fuente]_[entidad]`
- Dimensiones: `dim_[entidad]`
- Hechos: `fct_[entidad]`
- Análisis: `[entidad]_[análisis]`

### 12.2. Documentación

Documenta tus modelos en los archivos `schema.yml`:

```yaml
version: 2

models:
  - name: dim_customers
    description: "Dimensión de clientes con métricas agregadas"
    columns:
      - name: customer_id
        description: "Identificador único del cliente"
        tests:
          - unique
          - not_null
```

### 12.3. Pruebas

Crea pruebas para garantizar la calidad de los datos:
- Pruebas genéricas: unique, not_null, relationships, accepted_values
- Pruebas personalizadas: archivos SQL en la carpeta `tests/`

## 13. Recursos Adicionales

- [Documentación oficial de DBT](https://docs.getdbt.com/)
- [Documentación de DuckDB](https://duckdb.org/docs/)
- [Guía de estilo de DBT](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)
- [Mejores prácticas para pruebas](https://docs.getdbt.com/docs/build/tests)
- [Comunidad DBT en Slack](https://community.getdbt.com/)

## Conclusión

Este tutorial te ha proporcionado una introducción completa al uso de DBT con DuckDB para análisis de datos. Has aprendido a configurar el proyecto, ejecutar modelos, realizar pruebas y utilizar técnicas avanzadas para transformar tus datos de manera eficiente y mantenible.

La combinación de DBT con DuckDB es particularmente potente para análisis de datos local y desarrollo rápido de flujos de trabajo de datos. A medida que te familiarices con estas herramientas, podrás desarrollar flujos de trabajo más complejos y obtener insights más profundos de tus datos.
