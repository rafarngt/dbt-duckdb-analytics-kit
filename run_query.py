#!/usr/bin/env python
import duckdb
import sys

def main():
    if len(sys.argv) < 2:
        print("Uso: python run_query.py 'SELECT * FROM tabla LIMIT 10'")
        return
    
    query = sys.argv[1]
    conn = duckdb.connect('data/analytics_dev.duckdb')
    
    try:
        results = conn.execute(query).fetchdf()
        print(results)
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 