#!/usr/bin/env python
import os
import sys
import duckdb
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Uso: python run_analysis.py <nombre_del_análisis>")
        print("Ejemplo: python run_analysis.py customer_analysis")
        return
    
    analysis_name = sys.argv[1]
    project_name = "dbt_duckdb_analytics_kit"  # Nombre del proyecto DBT
    
    # Ruta al archivo SQL compilado
    sql_path = f"target/compiled/{project_name}/analyses/{analysis_name}.sql"
    
    if not os.path.exists(sql_path):
        print(f"Error: No se encontró el archivo {sql_path}")
        print("Asegúrate de haber ejecutado 'dbt compile' primero.")
        return
    
    # Leer el contenido del archivo SQL
    with open(sql_path, 'r') as f:
        query = f.read()
    
    # Conectar a DuckDB y ejecutar la consulta
    try:
        conn = duckdb.connect('data/analytics_dev.duckdb')
        results = conn.execute(query).fetchdf()
        
        # Mostrar los resultados en formato tabular
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        print(f"\n=== Resultados del análisis '{analysis_name}' ===\n")
        print(results)
        
        # Guardar los resultados en un CSV si se solicita
        if len(sys.argv) > 2 and sys.argv[2] == '--save':
            output_dir = "analysis_results"
            os.makedirs(output_dir, exist_ok=True)
            output_file = f"{output_dir}/{analysis_name}_results.csv"
            results.to_csv(output_file, index=False)
            print(f"\nResultados guardados en {output_file}")
        
    except Exception as e:
        print(f"Error al ejecutar el análisis: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 