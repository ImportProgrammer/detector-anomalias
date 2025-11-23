"""
Utilidades de conexión a PostgreSQL
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from config import PROJECT_CONFIG

@st.cache_resource
def get_engine():
    """Crear conexión SQLAlchemy (cacheada para reutilización)"""
    postgres_config = PROJECT_CONFIG['postgres']
    
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    
    return create_engine(connection_string)

def get_connection():
    """Obtener conexión psycopg2 para queries específicos"""
    postgres_config = PROJECT_CONFIG['postgres']
    
    return psycopg2.connect(
        host=postgres_config['host'],
        port=postgres_config['port'],
        database=postgres_config['database'],
        user=postgres_config['user'],
        password=postgres_config['password']
    )

@st.cache_data(ttl=300)  # Cache por 5 minutos
def execute_query(query, params=None):
    """
    Ejecuta query y retorna DataFrame
    
    Args:
        query (str): Query SQL
        params (tuple): Parámetros para el query
    
    Returns:
        pd.DataFrame: Resultado del query
    """
    engine = get_engine()
    
    try:
        if params:
            df = pd.read_sql(query, engine, params=params)
        else:
            df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error ejecutando query: {e}")
        return pd.DataFrame()

def execute_query_dict(query, params=None):
    """
    Ejecuta query y retorna lista de diccionarios
    
    Args:
        query (str): Query SQL
        params (tuple): Parámetros para el query
    
    Returns:
        list: Lista de diccionarios con resultados
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(row) for row in results]
    except Exception as e:
        st.error(f"Error ejecutando query: {e}")
        cursor.close()
        conn.close()
        return []

@st.cache_data(ttl=60)  # Cache por 1 minuto
def test_connection():
    """
    Prueba la conexión a la base de datos
    
    Returns:
        bool: True si la conexión es exitosa
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return False