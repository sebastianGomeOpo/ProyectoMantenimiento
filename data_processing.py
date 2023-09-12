# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 08:07:53 2023

@author: Sebastian Gomez
"""
import pandas as pd
import hashlib
import numpy as np


# -------------------------
# Funciones de Carga de Datos
# -------------------------
def load_data(file_path, file_type='excel'):
    """
    Carga datos desde un archivo de Excel.

    Parámetros:
    - ruta_archivo (str): Ruta al archivo de Excel.

    Retorna:
    - DataFrame: Datos cargados.
    """
    if file_type == 'excel':
        return pd.read_excel(file_path)
    elif file_type == 'csv':
        return pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file type.")


# -------------------------
# Funciones de Conversión y Transformación
# -------------------------

def generate_hash_id(*args):
    """Genera un HASH ID en base a una entrada."""
    combined_string = ''.join(map(str, args))
    return hashlib.sha256(combined_string.encode()).hexdigest()


def convert_column_to_int(df, column_name):
    """
    Convierte una columna específica a entero sin decimales.
    Los valores NaN se mantienen intactos.

    Parámetros:
    - df (pd.DataFrame): DataFrame a procesar.
    - column_name (str): Nombre de la columna a convertir.

    Devuelve:
    - pd.DataFrame: DataFrame con la columna convertida.
    - str: Mensaje sobre el proceso realizado.
    """
    mensaje = ""
    try:
        # Convertir a float primero para eliminar el ".0" al final si es necesario
        df[column_name] = df[column_name].astype(float)

        # Reemplazar NaNs por un valor ficticio
        df[column_name].fillna(1, inplace=True)

        # Convertir la columna a int64
        df[column_name] = df[column_name].astype(np.int64)

        # Convertir los valores ficticios de nuevo a NaN
        df[column_name].replace(1, np.nan, inplace=True)

    except Exception as e:
        mensaje = f"Error al convertir la columna '{column_name}': {str(e)}"

    return df, mensaje


def convert_column(column, desired_type):
    """
    Convierte el tipo de datos de una columna.

    Parámetros:
    - columna (Series): La columna a convertir.
    - tipo_deseado (str): El tipo de datos objetivo.

    Retorna:
    - Series: Columna convertida.
    """
    try:
        return column.astype(desired_type)
    except ValueError:
        if desired_type == 'float':
            return column.apply(lambda x: str(x) if not isinstance(x, (int, float)) else x)
        return column


def set_column_dtypes(data, column_type_mapping):
    """
    Establece tipos de datos específicos para columnas en el DataFrame.

    Parámetros:
    - datos (DataFrame): El DataFrame.
    - mapeo_tipo_columna (dict): Diccionario que mapea nombres de columnas a tipos de datos deseados.

    Retorna:
    - None
    """
    for column_name, data_type in column_type_mapping.items():
        if column_name in data.columns:
            try:
                data[column_name] = data[column_name].astype(data_type)
            except ValueError:
                # Si hay un error al convertir a 'float', convierte la columna a 'str'
                if data_type == 'float':
                    data[column_name] = data[column_name].astype(str)


# -------------------------
# Funciones de Manipulación de DataFrames
# -------------------------

def process_MCBE(df):
    """
    Procesa el DataFrame asociado al archivo MCBE.
    """
    # Eliminar la primera columna
    df = df.drop(columns=df.columns[0])

    # Usar la tercera fila como nombres de columnas
    df.columns = df.iloc[1]

    # Eliminar la tercera, cuarta y quinta fila
    df = df.drop(df.index[0:4])
    # Reseteamos el índice para un mejor aspecto
    df = df.reset_index(drop=True)
    return df


def load_and_unify_dataframes(path_pen, path_eur):
    """
    Carga y fusiona dos CSVs basados en mes y año para tasas de cambio de divisas.

    Parámetros:
    - ruta_pen (str): Ruta al CSV PEN a USD.
    - ruta_eur (str): Ruta al CSV EUR a USD.

    Retorna:
    - DataFrame: DataFrame fusionado con tasas de cambio para PEN y EUR.
    """

    def load_and_rename_csv(path, column_names):
        """Carga un CSV y renombra sus columnas."""
        data = pd.read_csv(path, skiprows=2, encoding='ISO-8859-1')
        data.columns = column_names
        return data

    def extract_year_month_from_date(data):
        """Extrae año y mes de una columna 'Fecha'."""
        data['Fecha'] = pd.to_datetime(data['Fecha'])
        data['Año'] = data['Fecha'].dt.year
        data['Mes'] = data['Fecha'].dt.month
        data.drop('Fecha', axis=1, inplace=True)
        return data

    # Cargar y validar los datasets
    try:
        df_pen = load_and_rename_csv(path_pen, ['Fecha', 'Tipo_Cambio_PEN'])
        df_eur = load_and_rename_csv(path_eur, ['Fecha', 'Tipo_Cambio_EUR'])
    except Exception as e:
        raise ValueError(f"Error al cargar y renombrar los datasets: {e}")

    # Handle conversion from USD to EUR
    df_eur['Tipo_Cambio_EUR'] = 1 / df_eur['Tipo_Cambio_EUR']

    df_pen = extract_year_month_from_date(df_pen)
    df_eur = extract_year_month_from_date(df_eur)

    merged_data = pd.merge(df_pen, df_eur, on=['Mes', 'Año'], how='outer')
    return merged_data


def rename_column(data, current_name, new_name):
    """
    Renombra una columna en un DataFrame.

    Parámetros:
    - datos (DataFrame): El DataFrame.
    - nombre_actual (str): Nombre actual de la columna.
    - nuevo_nombre (str): Nuevo nombre de la columna.

    Retorna:
    - DataFrame: DataFrame con columna renombrada.
    """
    data.rename(columns={current_name: new_name}, inplace=True)
    return data


def left_join(data1, data2, on_column, columns_to_join):
    """
    Realiza una unión izquierda entre dos DataFrames.

    Parámetros:
    - datos1 (DataFrame): DataFrame izquierdo.
    - datos2 (DataFrame): DataFrame derecho.
    - en_columna (str): Columna para unir.
    - columnas_a_unir (list): Columnas para incluir del DataFrame derecho.

    Retorna:
    - DataFrame: Resultado de la unión izquierda.
    """
    return pd.merge(data1, data2[[on_column] + columns_to_join], on=on_column, how='left')


def outer_join_and_fill_na(data1, data2, on_column, fill_with=''):
    """
    Realiza una unión exterior y rellena valores faltantes.

    Parámetros:
    - datos1 (DataFrame): DataFrame izquierdo.
    - datos2 (DataFrame): DataFrame derecho.
    - en_columna (str): Columna para unir.
    - llenar_con (str, opcional): Valor para llenar las entradas faltantes. Por defecto, es una cadena vacía.

    Retorna:
    - DataFrame: Resultado de la unión exterior con valores faltantes rellenados.
    """

    result = pd.merge(data1, data2[[on_column]], on=on_column, how='outer')
    result.fillna(fill_with, inplace=True)
    return result


# -------------------------
# Funciones de Cálculo y Creación de Columnas
# -------------------------

def validate_and_create_comodin_columns(df, df_name):
    """
    Valida y crea las columnas 'COMODIN' en un DataFrame basado en las especificaciones dadas, usando un hash SHA256.
    """

    # Función para establecer los tipos de columnas según df_name
    def _set_column_dtypes(df, df_name):
        column_types = {}
        if df_name == 'df_ME5A':
            column_types = {
                'Solicitud de pedido': 'float',
                'Material': 'str',
                'Pos.solicitud pedido': 'int',
                'Posición de pedido': 'int',
                'Pedido': 'float'
            }
        elif df_name == 'df_ME2N':
            column_types = {
                'Documento compras': 'float',
                'Material': 'str',
                'Posición': 'int'
            }
        elif df_name == 'df_ZMM621':
            column_types = {
                'Nro Pedido': 'float',
                'Material': 'str',
                'Pos. Pedido': 'int'
            }

        for col, col_type in column_types.items():
            try:
                df[col] = df[col].astype(col_type)
            except Exception as e:
                print(f"Error convirtiendo la columna '{col}' al tipo '{col_type}': {e}")
        return df

    # Función para procesar las columnas 'COMODIN'
    def _process_comodins(df, comodin_specs):
        mensajes = []
        specs = comodin_specs.get(df_name, {})
        for comodin, cols in specs.items():
            try:
                df[comodin] = df[cols].apply(lambda row: generate_hash_id(*row), axis=1)
                mensaje_success = f"En {df_name}: Se ha creado/actualizado la columna {comodin}."
                mensajes.append(mensaje_success)
            except Exception as e:
                mensaje_error = f"Error inesperado en {df_name} al procesar la columna {comodin}: {str(e)}"
                mensajes.append(mensaje_error)
        return df, mensajes

    # Cuerpo principal de la función
    if df_name == 'df_ME5A':
        df, message = convert_column_to_int(df, 'Pedido')

    df = _set_column_dtypes(df, df_name)

    comodin_specs = {
        'df_ME5A': {
            'COMODIN SOLPED': ['Solicitud de pedido', 'Material', 'Pos.solicitud pedido'],
            'COMODIN OC': ['Pedido', 'Material', 'Posición de pedido']
        },
        'df_ME2N': {
            'COMODIN OC': ['Documento compras', 'Material', 'Posición']
        },
        'df_ZMM621': {
            'COMODIN OC': ['Nro Pedido', 'Material', 'Pos. Pedido']
        }
    }

    df, mensajes = _process_comodins(df, comodin_specs)

    return df, "\n\n".join(mensajes)


def vectorized_calcular_estado_contable(df):
    """Vectorized version of calcular_estado_contable."""
    conditions = [
        df['Pedido'].isna(),
        df['Fecha contable'].isna()
    ]
    choices = ['SIN OC', 'CON OC NO CONTAB']

    return np.select(conditions, choices, default='CONTABILIZADO')


def categorizar_tipo(entry):
    """
    Categorize the type of transaction based on the given entry.

    Parameters:
    - entry (str): Entry to categorize.

    Returns:
    - str: Transaction type.
    """
    if pd.isna(entry) or entry in ['', 'nan']:
        return 'SERVICIO'
    return 'COMPRA'


def categorizar_entrega(value):
    """
    Categoriza el estado de entrega basado en el valor dado.

    Parámetros:
    - valor (float): Valor que indica el estado de entrega.

    Retorna:
    - str: Estado de entrega.
    """
    if pd.isna(value) or value in ['', 'nan'] or value > 0:
        return 'PENDIENTE'  # Return NaN or blank cell
    return 'CONCLUIDO'


# Vectorized versions for strategy functions

def vectorized_handle_strategy_02(indicators):
    """Vectorized version for handle_strategy_02."""
    conditions = [
        indicators == "",
        indicators == "X",
        indicators == "XX"
    ]
    choices = ["COMPRADOR", "GERENTE ADM", "GERENTE GENERAL"]
    return np.select(conditions, choices, default=np.nan)


def vectorized_handle_strategy_00(indicators):
    """Vectorized version for handle_strategy_00."""
    conditions = [
        indicators == "",
        indicators == "X"
    ]
    choices = ["COMPRADOR", "JEFE LOGÍSTICA"]
    return np.select(conditions, choices, default=np.nan)


def vectorized_handle_strategy_01(indicators):
    """Vectorized version for handle_strategy_01."""
    conditions = [
        indicators == "",
        indicators == "X"
    ]
    choices = ["COMPRADOR", "GERENTE ADM"]
    return np.select(conditions, choices, default=np.nan)


def vectorized_calculate_status(df):
    """Vectorized version of calculate_status."""
    strategy_handlers = {
        2: vectorized_handle_strategy_02,
        0: vectorized_handle_strategy_00,
        1: vectorized_handle_strategy_01
    }

    # Default status based on "Por entregar (cantidad)"
    status = np.where(df['Por entregar (cantidad)'] == "CONCLUIDO", "", "SIN OC")

    for strategy, handler in strategy_handlers.items():
        mask = df['ESTRATÉGIA DE LIBERACIÓN'] == strategy
        status[mask] = handler(df.loc[mask, 'Estado liberación'])

    return status


def vectorized_calculate_date_difference(row):
    """Calculate the difference in days between 'Fecha de OC' and 'Fecha de SOLPED'."""
    date_of_oc = pd.to_datetime(row['Fecha de OC'], errors='coerce')
    date_of_solped = pd.to_datetime(row['Fecha de SOLPED'], errors='coerce')

    default_diff = np.where(pd.isnull(date_of_oc) & pd.isnull(date_of_solped), "",
                            np.where(pd.isnull(date_of_oc), (pd.Timestamp.today() - date_of_solped).dt.days,
                                     (date_of_oc - date_of_solped).dt.days))

    return default_diff


def vectorized_calculate_days_difference(df):
    """Vectorized version of calculate_days_difference."""
    date_of_oc = pd.to_datetime(df['Fecha de OC'], errors='coerce')
    date_of_last_release = pd.to_datetime(df['Fecha de aprobación de la orden de compr'], errors='coerce')

    no_oc_diff = np.where(df['PENDIENTE DE LIBERACIÓN DE OC'] == "SIN OC", "",
                          (pd.Timestamp.today() - date_of_oc).dt.days)
    oc_diff = np.where(df['PENDIENTE DE LIBERACIÓN DE OC'], (date_of_last_release - date_of_oc).dt.days, no_oc_diff)

    return oc_diff


######################################################################################################

def vectorized_tipoCromprometido(df):
    """Vectorized version of tipoCromprometido."""

    # Defining conditions for each category
    condiciones_servicio_por_finalizar = (
            (df['TIPO'] == 'SERVICIO') &
            (df['Por entregar (STATUS)'] == 'PENDIENTE') &
            (df['Indicador de borrado SOLPED'] != 'SOLPED BORRADA') &
            (df['Indicador de borrado Orden de Compra'] != 'OC.BORRADA')
    )

    condiciones_compra_por_llegar = (
            (df['TIPO'] == 'COMPRA') &
            (df['Por entregar (STATUS)'] == 'PENDIENTE') &
            (df['Por entregar (cantidad)'] > 0) &
            (df['Indicador de borrado SOLPED'] != 'SOLPED BORRADA') &
            (df['Indicador de borrado Orden de Compra'] != 'OC.BORRADA') &
            (df['Estado factura'] != 'SIN OC')
    )

    condiciones_compra_por_retirar = (
            (df['TIPO'] == 'COMPRA') &
            (df['Por entregar (STATUS)'] == 'CONCLUIDO') &
            (df['Indicador de borrado SOLPED'] != 'SOLPED BORRADA') &
            (df['Indicador de borrado Orden de Compra'] != 'OC.BORRADA') &
            (df['Por entregar (cantidad)'] == 0) &
            (df['Libre utilización'] > 0)
    )

    # Use np.select to assign categories based on conditions
    conditions = [condiciones_servicio_por_finalizar, condiciones_compra_por_llegar, condiciones_compra_por_retirar]
    choices = ["SERV. POR FINALIZAR", "COMPRA POR LLEGAR", "COMPRA POR RETIRAR"]

    return np.select(conditions, choices, default="")


def vectorized_calcular_fecha(df):
    """Vectorized version of calcular_fecha."""
    conditions = [
        df['Estado contable'] == "CONTABILIZADO",
        df['Estado contable'] == "CON OC NO CONTAB",
        df['Estado contable'] == "SIN OC"
    ]

    choices = [
        df['Fecha contable'].astype(str),
        df['Fecha de aprobación de la orden de compr'].astype(str),
        df['Fecha de SOLPED'].astype(str)
    ]

    return np.select(conditions, choices, default="")


def vectorized_get_tipo_cambio(df):
    """Vectorized version of get_tipo_cambio."""
    return np.where(df['Moneda'] == "PEN", df['Tipo_Cambio_PEN'],
                    np.where(df['Moneda'] == "EUR", df['Tipo_Cambio_EUR'], 1))


def vectorized_convertir_moneda(df):
    """Vectorized version of convertir_moneda."""
    return df['Precio neto'] / df['Tipo de Cambio']


def sort_and_remove_duplicates(data, columns_to_sort, duplicate_check_column):
    """
    Ordena un DataFrame y elimina filas duplicadas según una columna específica.

    Parámetros:
    - datos (DataFrame): El DataFrame a procesar.
    - columnas_a_ordenar (list): Lista de columnas para ordenar.
    - columna_verificacion_duplicados (str): Columna para verificar duplicados.

    Retorna:
    - DataFrame: DataFrame procesado.
    """
    sorted_data = data.sort_values(by=columns_to_sort, ascending=[True, False])
    sorted_data.drop_duplicates(subset=duplicate_check_column, keep='first', inplace=True)
    return sorted_data


def process_material(value):
    """Procesa el valor del material según las especificaciones dadas."""
    # Si el valor es NaN, simplemente regresa NaN
    if pd.isna(value):
        return value

    # Si el valor es completamente numérico, quita los ceros de la izquierda
    if str(value).isnumeric():
        return str(int(value))

    # Si no, simplemente regresa el valor original
    return str(value)


def process_dataframes_for_join(df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE):
    """Prepara DataFrames para las operaciones de join."""
    column_types = {
        'Fecha de solicitud': 'datetime64[D]',
        'Fecha de reg. Factura': 'datetime64[D]',
        'Fecha de HES/EM': 'datetime64[D]',
        'Fecha contable': 'datetime64[D]',
        'Fecha de aprobación de la orden de compr': 'datetime64[D]',
        'Fecha documento': 'datetime64[D]',
        # Añade cualquier otra columna que necesites definir aquí
    }

    set_column_dtypes(df_ME5A, column_types)
    set_column_dtypes(df_ZMM621_fechaAprobacion, column_types)
    set_column_dtypes(df_IW38, column_types)
    set_column_dtypes(df_ME2N_OC, column_types)
    set_column_dtypes(df_ZMB52, column_types)
    set_column_dtypes(df_MCBE, column_types)

    df_ZMM621_fechaHES_HEM = sort_and_remove_duplicates(df_ZMM621_fechaAprobacion, ['COMODIN OC', 'Fecha de HES/EM'],
                                                        'COMODIN OC')
    df_ZMM621_fechaHES_HEM['Estado HES/HEM'] = df_ZMM621_fechaHES_HEM['Fecha de HES/EM'].apply(
        lambda x: "SIN HES/EM" if pd.isna(x) else "ACEPTADO")

    # df_ZMM621_Ordenes = df_ZMM621_fechaAprobacion[['COMODIN SOLPED', 'Orden']].drop_duplicates()

    df_ZMB52 = df_ZMB52.pivot_table(
        index=['Material'],
        values=['Valor libre util.', 'Libre utilización'],
        aggfunc='sum').reset_index()

    df_ZMB52['Material'] = df_ZMB52['Material'].apply(process_material)
    df_ME5A['Material'] = df_ME5A['Material'].apply(process_material)

    # Devuelve df_MCBE también:
    return df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_ZMM621_fechaHES_HEM


def create_ZMM621_COMODIN_OC_unique(df_ZMM621_fechaAprobacion):
    """
    Crea el dataframe ZMM621 , usando la columna 'COMODIN OC'como llave primary

    Parámetros:
    - df_ZMM621(DataFrame): Se carga los datos directamente cargados del SAP 

    Retorna:
    - DataFrame: DataFrame procesado.
    """
    data_unique_ZMM621 = sort_and_remove_duplicates(df_ZMM621_fechaAprobacion, ['COMODIN OC', 'Fecha contable'],
                                                    'COMODIN OC')
    data_unique_ZMM621['Estado factura'] = data_unique_ZMM621['Fecha de reg. Factura'].apply(
        lambda x: "SIN FACTURA" if pd.isna(x) else "FACTURADO")
    return data_unique_ZMM621


def create_ZMM621_Orden_unique(df_ZMM621_fechaAprobacion):
    data_unique_ZMM621_Orden = sort_and_remove_duplicates(df_ZMM621_fechaAprobacion,
                                                          ['Orden de mantenimiento', 'Fecha contable'],
                                                          'Orden de mantenimiento')
    rename_column(data_unique_ZMM621_Orden, 'Orden de mantenimiento', 'Orden')
    return data_unique_ZMM621_Orden

def check_columns_existence(df, columns):
    for col in columns:
        if col not in df.columns:
            raise KeyError(f"Column {col} not found in DataFrame!")
            
def merge_dataframes(df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_ZMM621_fechaHES_HEM,
                     df_tipos_cambio):
    """Merge DataFrames based on the provided logic."""
    joined_data = df_ME5A[['COMODIN SOLPED', 'COMODIN OC']]
    # joined_data = outer_join_and_fill_na(joined_data, df_ME2N_OC, 'COMODIN OC').drop_duplicates()
    df_ZMM621_OCompras = create_ZMM621_COMODIN_OC_unique(df_ZMM621_fechaAprobacion)
    df_ZMM621_OMant = create_ZMM621_Orden_unique(df_ZMM621_fechaAprobacion)

    # joined_data = outer_join_and_fill_na(joined_data, df_ZMM621_fechaAprobacion, 'COMODIN SOLPED').drop_duplicates()
    joined_data = left_join(joined_data, df_ZMM621_OMant, 'COMODIN OC', ['Orden']).drop_duplicates()

    # joined_data = outer_join_and_fill_na(joined_data, df_IW38, 'Orden')

    # Perform left joins to extract data from transactions
    left_join_operations = [
        (df_ME5A, 'COMODIN SOLPED',
         ['Material', 'Pedido', 'Solicitud de pedido', 'Pos.solicitud pedido', 'SOLICITANTE', 'Indicador de borrado',
          'Indicador de Liberación', 'Fecha de solicitud', 'Unidad de medida', 'Cantidad solicitada', 'Texto breve']),
        (df_ZMM621_OCompras, 'COMODIN OC',
         ['Estado factura', 'Fecha de reg. Factura', 'Fecha de HES/EM', 'Fecha contable',
          'Condición de pago del pedido', 'Fecha de aprobación de la orden de compr', 'Valor net. Solped',
          'N° Activo']),
        (df_IW38, 'Orden',
         ['Pto.tbjo.responsable', 'Denominación de la ubicación técnica', 'Denominación de objeto técnico', 'Equipo']),
        (df_ME2N_OC, 'COMODIN OC',
         ['Proveedor/Centro suministrador', 'Estado liberación', 'Indicador de borrado', 'Fecha documento',
          'Por entregar (cantidad)', 'Cantidad de pedido', 'Precio neto', 'Moneda', 'Por entregar (valor)',
          'Ind.liberación', 'ESTRATÉGIA DE LIBERACIÓN']),
        (df_ZMB52, 'Material', ['Libre utilización']),
        (df_MCBE, 'Material', ['Últ.salida', 'Últ.cons.', 'Últ.mov.']),
        (df_ZMM621_fechaHES_HEM, 'COMODIN OC', ['Estado HES/HEM'])
    ]

    for df, key, columns in left_join_operations:
        check_columns_existence(df, [key]+ columns)
        joined_data = left_join(joined_data, df, key, columns)

    return joined_data


def refine_joined_data(joined_data):
    """Refine the joined data by renaming columns and filling missing values."""
    rename_mappings = {
        'Fecha de solicitud': 'Fecha de SOLPED',
        'Indicador de borrado_x': 'Indicador de borrado SOLPED',
        'Indicador de borrado_y': 'Indicador de borrado Orden de Compra',
        'Texto breve': 'Descripcion Material',
        'Fecha documento': 'Fecha de OC'
    }

    for current_name, new_name in rename_mappings.items():
        rename_column(joined_data, current_name, new_name)

    fill_mappings = {
        'Estado factura': 'SIN OC',
        'Estado HES/HEM': 'SIN OC'
    }

    for col, value in fill_mappings.items():
        joined_data[col].fillna(value, inplace=True)

    return joined_data


def calculate_additional_columns(joined_data, df_tipos_cambio):
    """Calculate and add new columns based on the provided logic."""
    joined_data['Estado contable'] = vectorized_calcular_estado_contable(joined_data)
    joined_data['Indicador de borrado SOLPED'] = np.where(joined_data['Indicador de borrado SOLPED'] == 'True',
                                                          'SOLPED BORRADA ', '')
    joined_data['Indicador de borrado Orden de Compra'] = np.where(
        joined_data['Indicador de borrado Orden de Compra'] == 'L', 'OC.BORRADA', '')
    joined_data['TIPO'] = np.where(joined_data['Material'].isna() | joined_data['Material'].isin(['', 'nan']),
                                   'SERVICIO', 'COMPRA')
    # joined_data['TIPO'] = joined_data['Material'].apply(categorizar_tipo)
    mask = joined_data['Por entregar (cantidad)'].isna() | joined_data['Por entregar (cantidad)'].isin(['', 'nan']) | (
                joined_data['Por entregar (cantidad)'] > 0)
    joined_data['Por entregar (STATUS)'] = np.where(mask, 'PENDIENTE', 'CONCLUIDO')
    # joined_data['Por entregar (STATUS)'] = joined_data['Por entregar (cantidad)'].apply(categorizar_entrega)
    joined_data['PENDIENTE DE LIBERACIÓN DE OC'] = vectorized_calculate_status(joined_data)
    # joined_data['DEMORA EN GENERAR OC (DIAS)'] = joined_data.apply(calculate_date_difference, axis=1)
    joined_data['DEMORA EN GENERAR OC (DIAS)'] = vectorized_calculate_date_difference(joined_data)
    # joined_data['DEMORA EN LIBERACIONES DE OC'] = joined_data.apply(calculate_days_difference, axis=1)
    joined_data['DEMORA EN LIBERACIONES DE OC'] = vectorized_calculate_days_difference(joined_data)
    # joined_data['FECHA COMODIN - CONTABLE O SOLPED'] = joined_data.apply(calcular_fecha, axis=1)
    joined_data['FECHA COMODIN - CONTABLE O SOLPED'] = vectorized_calcular_fecha(joined_data)
    joined_data['Año OC'] = pd.to_datetime(joined_data['Fecha de OC']).dt.year
    joined_data['Mes OC'] = pd.to_datetime(joined_data['Fecha de OC']).dt.month
    joined_data = pd.merge(joined_data, df_tipos_cambio, left_on=['Año OC', 'Mes OC'], right_on=['Año', 'Mes'],
                           how='left')
    # joined_data['Tipo de Cambio'] = joined_data.apply(get_tipo_cambio, axis=1)
    joined_data['Tipo de Cambio'] = vectorized_get_tipo_cambio(joined_data)
    joined_data['Precio neto'] = pd.to_numeric(joined_data['Precio neto'], errors='coerce')
    joined_data['Tipo de Cambio'] = pd.to_numeric(joined_data['Tipo de Cambio'], errors='coerce')
    # joined_data['Precio Convertido Dolares'] = joined_data.apply(convertir_moneda, axis=1)
    joined_data['Precio Convertido Dolares'] = vectorized_convertir_moneda(joined_data)
    joined_data = joined_data.drop(columns=['Tipo_Cambio_PEN', 'Tipo_Cambio_EUR', 'Año', 'Mes'])
    # Falta logica
    joined_data['TIPO COMPROMETIDO SUGERENCIA'] = vectorized_tipoCromprometido(joined_data)
    return joined_data


def costoComprasPorRetirar(df):
    # Seleccionamos y ordenamos las columnas de interés
    df_filtered = df[['Descripcion Material', 'Fecha de OC', 'Precio Convertido Dolares', 'Libre utilización',
                      'Cantidad de pedido']].copy()
    df_filtered = df_filtered.sort_values(by=['Descripcion Material', 'Fecha de OC'], ascending=[True, False])
    # Calculamos el Precio Unitario
    df_filtered['Precio Unitario'] = df_filtered['Precio Convertido Dolares'] / df_filtered['Cantidad de pedido']
    # Tomamos solo la fecha contable más reciente por material
    df_latest = df_filtered.drop_duplicates(subset='Descripcion Material', keep='first')
    # Hacemos un left join entre el df original y df_latest para agregar la columna "Precio Unitario" al df original
    df = pd.merge(df, df_latest[['Descripcion Material', 'Precio Unitario']], on='Descripcion Material', how='left')
    # Hacemos el cálculo de Costo compras por retirar con la nueva columna "Precio Unitario"
    df['Costo compras por retirar'] = df['Precio Unitario'] * df['Libre utilización']
    return df


# -------------------------
# Función Principal de Procesamiento
# -------------------------
def process_data(df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_criticos,
                 df_inmovilizados, df_tipos_cambio):
    # Prepare data for joining
    df_ME5A_converted, df_ZMM621_fechaAprobacion_converted, df_IW38_converted, df_ME2N_OC_converted, df_ZMB52_converted, df_MCBE_converted, df_ZMM621_fechaHES_HEM_converted = process_dataframes_for_join(
        df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE)
    # Merge the dataframes
    joined_data = merge_dataframes(df_ME5A_converted, df_ZMM621_fechaAprobacion_converted, df_IW38_converted,
                                   df_ME2N_OC_converted, df_ZMB52_converted, df_MCBE_converted,
                                   df_ZMM621_fechaHES_HEM_converted, df_tipos_cambio)

    # Refine the merged data
    joined_data = refine_joined_data(joined_data)

    # Calculate additional columns
    joined_data = calculate_additional_columns(joined_data, df_tipos_cambio)

    column_order = [
        'COMODIN OC', 'COMODIN SOLPED', 'Ind.liberación', 'TIPO', 'SOLICITANTE', 'Pto.tbjo.responsable',
        'FECHA COMODIN - CONTABLE O SOLPED', 'Estado HES/HEM', 'Fecha de reg. Factura', 'Estado factura',
        'Fecha contable', 'Estado contable', 'Fecha de HES/EM', 'Pos.solicitud pedido', 'Material', 'N° Activo',
        'Cantidad solicitada', 'Unidad de medida', 'Solicitud de pedido', 'Cantidad de pedido',
        'Por entregar (cantidad)', 'Pedido', 'Indicador de Liberación', 'Orden', 'Condición de pago del pedido',
        'Valor net. Solped', 'Denominación de la ubicación técnica', 'Denominación de objeto técnico', 'Equipo',
        'Por entregar (STATUS)', 'Por entregar (valor)', 'Precio neto', 'Descripcion Material', 'Moneda',
        'Libre utilización', 'Indicador de borrado SOLPED', 'Año OC', 'Mes OC', 'Indicador de borrado Orden de Compra',
        'Fecha de SOLPED',
        'Fecha de OC', 'PENDIENTE DE LIBERACIÓN DE OC', 'Fecha de aprobación de la orden de compr',
        'DEMORA EN GENERAR OC (DIAS)', 'DEMORA EN LIBERACIONES DE OC', 'Proveedor/Centro suministrador',
        'Estado liberación', 'ESTRATÉGIA DE LIBERACIÓN', 'Tipo de Cambio', 'Precio Convertido Dolares',
        'TIPO COMPROMETIDO SUGERENCIA', 'Últ.mov.', 'Últ.cons.', 'Últ.salida']

    # Reorder the dataframe columns
    joined_data = joined_data[column_order]

    joined_data = costoComprasPorRetirar(joined_data)

    return joined_data
