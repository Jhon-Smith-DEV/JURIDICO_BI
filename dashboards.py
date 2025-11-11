import dash
from dash import dcc, html, dependencies as dd
import pandas as pd
import plotly.express as px
import pyodbc
import subprocess
import plotly.graph_objects as go
import os
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
import numpy as np

# ------------------- CONFIGURACIÓN BD --------------------
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "bi_juridico")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "YourStrong@Passw0rd")

CONNECTION_STRING = f"""
DRIVER={{ODBC Driver 18 for SQL Server}};
SERVER={DB_HOST},{DB_PORT};
DATABASE={DB_NAME};
UID={DB_USER};
PWD={DB_PASSWORD};
TrustServerCertificate=yes;
"""

QUERIES = {
    "ingresofecha": """
        SELECT fecha, monto
        FROM contratoServicio
        ORDER BY fecha;
    """,
    "ingresomateria": """
        SELECT cs.fecha, cs.monto, cj.meteria as materia
        FROM contratoServicio AS cs
        JOIN casoJuridico AS cj ON cs.nrocasojuridico = cj.nrocaso;
    """,
    "topclientes": """
        SELECT cs.fecha, cs.monto, c.nombre + ' ' + c.apellido AS nombre, c.ci
        FROM contratoServicio AS cs
        JOIN cliente AS c ON cs.cicliente = c.ci;
    """,
    "prediccion": """
        SELECT 
            DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1) AS fecha,
            COUNT(nrocontrato) AS total_contratos
        FROM contratoServicio
        GROUP BY YEAR(fecha), MONTH(fecha)
        ORDER BY fecha;
    """,
    "segmentacion": """
        SELECT 
            c.ci AS cliente_id,
            COUNT(cs.nrocontrato) AS frecuencia,
            AVG(cs.monto) AS monto_promedio,
            COUNT(DISTINCT cs.nrocasojuridico) AS variedad_casos
        FROM cliente c
        LEFT JOIN contratoServicio cs ON cs.cicliente = c.ci
        GROUP BY c.ci;
    """
}

def get_data():
    """
    Obtiene los datos desde SQL Server para los diferentes dashboards.

    Parámetros:
        - (no recibe parámetros)

    Proceso:
        - Conecta a la base de datos
        - Ejecuta las consultas SQL definidas en QUERIES
        - Convierte las tablas a DataFrames
        - Elimina registros nulos
        - Cierra la conexión

    Retorna:
        - dict[str, DataFrame] -> Un diccionario con un DataFrame por cada consulta
    """
    conn = pyodbc.connect(CONNECTION_STRING)
    dfs = {k: pd.read_sql_query(q, conn).dropna() for k, q in QUERIES.items()}
    conn.close()
    return dfs

# ------------------- PROCESADORES --------------------
def filtrar_por_fecha(df, fechaInicio, fechaFin):
    """
    Filtra un DataFrame por rango de fechas.

    Parámetros:
        df: DataFrame con una columna 'fecha'
        fechaInicio: fecha mínima permitida
        fechaFin: fecha máxima permitida

    Proceso:
        - Convierte 'fecha' a tipo datetime
        - Aplica el filtro entre ambas fechas

    Retorna:
        - DataFrame filtrado
    """
    df['fecha'] = pd.to_datetime(df['fecha'])
    return df[(df['fecha'] >= fechaInicio) & (df['fecha'] <= fechaFin)]


def graf_ingreso_fecha(df):
    """
    Procesa y grafica ingresos agrupados por mes.

    Parámetros:
        df: DataFrame con columnas ['fecha', 'monto']

    Proceso:
        - Convertir monto a numérico
        - Extraer el mes desde la fecha
        - Agrupar por mes y sumar montos
        - Crear gráfico de barras

    Retorna:
        tuple(DataFrame agrupado, float sumaTotal, Figure gráfico)
    """
    df['monto'] = pd.to_numeric(df['monto'])
    df['mes'] = df['fecha'].dt.to_period('M').dt.to_timestamp().dt.strftime('%m-%Y')
    df = df.groupby('mes', as_index=False)['monto'].sum()
    # redondear luego de la suma
    df['monto'] = df['monto'].round(2)
    # ordenar dataFrame por mes
    # df = df.sort_values(by='mes', ascending=True)
    print(df.tail())
    fig = px.bar(df, x='mes', y='monto', text='monto', color='mes')
    fig.update_traces(textposition='outside')
    return df, df['monto'].sum(), fig


def graf_ingreso_materia(df):
    """
    Agrupa ingresos por tipo de materia.

    Igual estructura de documentación que en la función anterior.
    """
    df['monto'] = pd.to_numeric(df['monto'])
    df = df.groupby('materia', as_index=False)['monto'].sum()
    fig = px.bar(df, x='materia', y='monto', text='monto', color='materia')
    fig.update_traces(textposition='outside')
    return df, df['monto'].sum(), fig


def graf_top_clientes(df):
    """
    Obtiene los 10 clientes con mayor monto total.

    Parámetros:
        df: DataFrame ['fecha', 'monto', 'nombre', 'ci']

    Proceso:
        - Convertir monto
        - Agrupar por cliente
        - Ordenar desc y tomar top 10

    Retorna:
        DataFrame top 10, suma total, gráfico de barras
    """
    df['monto'] = pd.to_numeric(df['monto'])
    df = df.groupby('ci', as_index=False).agg(nombre=('nombre', 'first'), monto=('monto','sum'))
    df = df.sort_values(by='monto', ascending=False).head(10)
    fig = px.bar(df, x='nombre', y='monto', text='monto', color='nombre')
    fig.update_traces(textposition='outside')
    return df, df['monto'].sum(), fig


def graf_prediccion(df):
    """
    Predice el número de contratos próximos meses usando regresión lineal.
    Entrada:
        df -> columnas ['fecha', 'total_contratos']
    Proceso:
        - Convertir 'fecha' a índice numérico
        - Entrenar modelo
        - Predecir próximos meses
        - Generar gráfico
    Retorna:
        df_original_con_predicciones, suma_total, figura
    """

    df['fecha'] = pd.to_datetime(df['fecha'])
    df['num_mes'] = np.arange(len(df))
    
    modelo = LinearRegression()
    modelo.fit(df[['num_mes']], df['total_contratos'])

    cantidadMesPredecir = 1
    futuros = pd.DataFrame({
        'num_mes': np.arange(len(df), len(df) + cantidadMesPredecir),
    })
    prediccion = pd.DataFrame({
        'fecha': pd.date_range(df['fecha'].max() + pd.DateOffset(months=1), periods=cantidadMesPredecir, freq='MS'),
        'total_contratos': modelo.predict(futuros[['num_mes']]).round(0)
    })
    # eliminar columna num_mes
    del df['num_mes']
    # concatenar dataFrame
    df_pred = pd.concat([df, prediccion], ignore_index=True)
    df_pred['fecha'] = df_pred['fecha'].dt.to_period('M').dt.to_timestamp().dt.strftime('%m-%Y')

    fig = px.bar(df_pred, x='fecha', y='total_contratos', text='total_contratos', color='fecha')
    fig.update_traces(textposition='outside')

    # return df_pred, df['total_contratos'].sum(), fig
    return df_pred, 0, fig


def graf_segmentacion(df):
    """
    Segmentación de clientes mediante K-Means.
    Entrada:
        df -> ['cliente_id', 'frecuencia', 'monto_promedio', 'variedad_casos']
    Proceso:
        - Llenar valores nulos
        - Escalar variables
        - Aplicar KMeans (k=3)
    Retorna:
        df con cluster asignado, suma, gráfico
    """
    df = df.fillna(0)

    modelo = KMeans(n_clusters=3, random_state=42)
    df['cluster'] = modelo.fit_predict(df[['frecuencia','monto_promedio','variedad_casos']])

    fig = px.scatter_3d(
        df,
        x='frecuencia',
        y='monto_promedio',
        z='variedad_casos',
        color='cluster',
        title='Segmentación de clientes'
    )

    return df, 0, fig


PROCESS_MAP = {
    "ingresofecha": graf_ingreso_fecha,
    "ingresomateria": graf_ingreso_materia,
    "topclientes": graf_top_clientes,
    "prediccion": graf_prediccion,
    "segmentacion": graf_segmentacion
}

# ---------------- DASHBOARD ----------------
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1(id='titulo', style={'textAlign': 'center'}),
    html.Div(
        dcc.DatePickerRange(
            id='rango-fecha',
            display_format='DD-MM-YYYY',
        ),
        style={'textAlign':'center',
               'border':'black, solid',
               'width':300,
               'marginLeft':'auto',
               'marginRight':'auto'}
    ),
    dcc.RadioItems(
        id='menu',
        options=[
            {'label': 'Ingresos por mes', 'value': 'ingresofecha'},
            {'label': 'Ingresos por materia', 'value': 'ingresomateria'},
            {'label': 'Top 10 clientes', 'value': 'topclientes'},
            {'label': 'Predicción de contratos', 'value': 'prediccion'},
            {'label': 'Segmentación de clientes', 'value': 'segmentacion'},
            {'label': 'Actualizar BD', 'value': 'actualizarbd'},
        ],
        value='ingresofecha',
        labelStyle={'display': 'inline-block', 'margin-right': '20px'},
        style={'textAlign': 'center'}
    ),
    html.Div(id='estado-sincronizacion', style={'textAlign': 'center'}),
    html.Div(id='suma-monto', style={'fontSize':28,'textAlign':'center'}),
    dcc.Graph(id='gbarras')
])

@app.callback(
    [dd.Output('titulo','children'),
     dd.Output('gbarras','figure'),
     dd.Output('estado-sincronizacion','children'),
     dd.Output('suma-monto','children')],
    [dd.Input('menu','value'),
     dd.Input('rango-fecha','start_date'),
     dd.Input('rango-fecha','end_date')]
)
def actualizar_vista(menu, fechaInicio, fechaFin):
    # fechaInicio="04/11/2022"
    # fechaFin="04/11/2025"
    if menu == 'actualizarbd':
        try:
            subprocess.run(['python','actualizar_bd.py'], check=True)
            return "Actualizando BD", go.Figure(), "✅ Actualizada", ""
        except:
            return "Error", go.Figure(), "❌ Error al actualizar", ""

    if not fechaInicio or not fechaFin:
        return "Seleccione fechas", go.Figure(), "", ""

    dfs = get_data()
    df = dfs[menu]
    # si el dataframe tiene columna llamada fecha entonces
    if 'fecha' in df.columns:
        df = filtrar_por_fecha(dfs[menu], fechaInicio, fechaFin)

    if df.empty:
        return "Sin datos", go.Figure(), "", ""

    df_proc, suma, fig = PROCESS_MAP[menu](df)
    # return menu.upper(), fig.update_traces(textposition='outside'), "", f"Total: BS. {round(suma,2)}"
    return menu.upper(), fig, "", f"Total: BS. {round(suma,2)}"

app.run(debug=True, host='0.0.0.0', port=8050)
