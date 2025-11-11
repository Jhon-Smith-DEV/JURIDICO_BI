"""
Módulo: sync_graphql_db.py
Descripción:
    Este programa obtiene datos desde un endpoint GraphQL,
    los procesa y almacena en una base de datos SQL Server.
    Posteriormente, otra aplicación podrá utilizar estos datos
    para generar reportes o visualizaciones.

Requisitos:
    - Variables de entorno configuradas:
        GRAPHQL_URL        : URL del servidor GraphQL
        DB_HOST            : Host de la base de datos
        DB_PORT            : Puerto de conexión
        DB_NAME            : Nombre de la base de datos
        DB_USER            : Usuario
        DB_PASSWORD        : Contraseña
"""

import os
import requests
import pandas as pd
import pyodbc


# ==============================
# 🔧 Configuración
# ==============================

# GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://127.0.0.1:8080/graphql")
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://host.docker.internal:8080/graphql")

# Configuración para SQL Server
DB_HOST = os.getenv("DB_HOST", "db")
# DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "bi_juridico")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "YourStrong@Passw0rd")

# Cadena de conexión para SQL Server
DB_CONNECTION_STRING = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={DB_HOST},{DB_PORT};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD};TrustServerCertificate=yes;"


# ==============================
# 🔍 Consultas GraphQL
# ==============================

QUERY_CLIENTES = """
query {
  allClientes {
    ci
    nombre
    apellido
  }
}
"""

QUERY_CASOS = """
query {
  allCasos {
    id
    meteria
  }
}
"""

QUERY_SERVICIOS = """
query {
  allContratos {
    id
    fecha
    precioBS
    cliente {
      ci
    }
    Caso {
      id
    }
  }
}
"""


# ==============================
# 🧩 Funciones auxiliares
# ==============================

def consulta_graphql(query: str) -> dict:
    """Ejecuta una consulta GraphQL y devuelve la respuesta como diccionario."""
    response = requests.post(GRAPHQL_URL, json={"query": query})
    if response.status_code != 200:
        raise Exception(f"Error al consultar GraphQL ({response.status_code})")
    return response.json()["data"]


def procesar_datos_servicios(raw_data: list[dict]) -> pd.DataFrame:
    """Procesa los servicios para convertirlos en un formato tabular limpio."""
    processed = []
    for servicio in raw_data:
        processed.append({
            "nrocontrato": servicio["id"],
            "fecha": servicio["fecha"],
            "monto": servicio["precioBS"],
            "cicliente": servicio["cliente"]["ci"] if servicio["cliente"] else None,
            "nrocasojuridico": servicio["Caso"]["id"] if servicio["Caso"] else None
        })
    return pd.DataFrame(processed)


def limpiar_dataframes(df_clientes: pd.DataFrame,
                       df_casos: pd.DataFrame,
                       df_servicios: pd.DataFrame):
    """Limpia y normaliza los datos: elimina duplicados, convierte tipos."""
    df_clientes = df_clientes.drop_duplicates(subset="ci").reset_index(drop=True)
    df_casos = df_casos.drop_duplicates(subset="id").rename(
        columns={"id": "nrocaso", "materia": "materia"}
    ).reset_index(drop=True)
    df_servicios = df_servicios.drop_duplicates(subset="nrocontrato").reset_index(drop=True)

    # Normalización de tipos
    df_servicios["fecha"] = pd.to_datetime(df_servicios["fecha"], errors="coerce")
    df_servicios["monto"] = pd.to_numeric(df_servicios["monto"], errors="coerce").round(2)

    return df_clientes, df_casos, df_servicios



# ==============================
# 🚀 Flujo principal
# ==============================

def main():
    """Flujo principal del programa."""
    print("🔄 Obteniendo datos desde GraphQL...")

    # Obtener datos del endpoint GraphQL
    data_clientes = consulta_graphql(QUERY_CLIENTES)["allClientes"]
    data_casos = consulta_graphql(QUERY_CASOS)["allCasos"]
    data_servicios = consulta_graphql(QUERY_SERVICIOS)["allContratos"]

    # Convertir a DataFrame
    df_clientes = pd.DataFrame(data_clientes)
    df_casos = pd.DataFrame(data_casos)
    df_servicios = procesar_datos_servicios(data_servicios)

    # Limpiar y normalizar los datos
    df_clientes, df_casos, df_servicios = limpiar_dataframes(df_clientes, df_casos, df_servicios)

    print("✅ Datos procesados correctamente:")
    print(f"Clientes: {df_clientes.shape}")
    print(df_clientes.head(3))

    print(f"Clientes: {df_casos.shape}")
    print(df_casos.head(3))

    print(f"Clientes: {df_servicios.shape}")
    print(df_servicios.head(3))

    # ---------------------------------------
    # 💾 Conexión a la base de datos
    # ---------------------------------------
    print("💾 Insertando datos en la base de datos...")
    
    try:
        with pyodbc.connect(DB_CONNECTION_STRING) as conn:
            cursor = conn.cursor()
            
            # --- Insertar CLIENTES ---
            insert_clientes = """
                MERGE INTO cliente AS target
                USING (VALUES (?, ?, ?)) AS source (ci, nombre, apellido)
                ON target.ci = source.ci
                WHEN NOT MATCHED THEN
                    INSERT (ci, nombre, apellido)
                    VALUES (source.ci, source.nombre, source.apellido);
            """
            for row in df_clientes.itertuples(index=False):
                cursor.execute(insert_clientes, row)

            # --- Insertar CASOS ---
            insert_casos = """
                MERGE INTO casoJuridico AS target
                USING (VALUES (?, ?)) AS source (nrocaso, meteria)
                ON target.nrocaso = source.nrocaso
                WHEN NOT MATCHED THEN
                    INSERT (nrocaso, meteria)
                    VALUES (source.nrocaso, source.meteria);
            """
            for row in df_casos.itertuples(index=False):
                cursor.execute(insert_casos, row)

            # --- Insertar SERVICIOS ---
            insert_servicios = """
                MERGE INTO contratoServicio AS target
                USING (VALUES (?, ?, ?, ?, ?)) AS source (nrocontrato, fecha, monto, cicliente, nrocasojuridico)
                ON target.nrocontrato = source.nrocontrato
                WHEN NOT MATCHED THEN
                    INSERT (nrocontrato, fecha, monto, cicliente, nrocasojuridico)
                    VALUES (source.nrocontrato, source.fecha, source.monto, source.cicliente, source.nrocasojuridico);
            """
            for row in df_servicios.itertuples(index=False):
                cursor.execute(insert_servicios, row)

            conn.commit()
            cursor.close()

    
        print("🎉 Sincronización completada exitosamente (solo nuevos registros insertados).")
    
    except Exception as e:
        raise Exception(f"Error al guardar los datos en la base de datos: {e}")

# ==============================
# 🏁 Punto de entrada
# ==============================

if __name__ == "__main__":
    main() 
