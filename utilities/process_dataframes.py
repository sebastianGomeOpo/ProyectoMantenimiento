import pandas as pd 
import numpy as np
import hashlib
from rapidfuzz import process
#--------------------------------------------
#FUNCIONES DE MANIPULACION DE DATASETS BRUTOS
#---------------------------------------------

def corregir_solicitantes_vectorizado(df, lista_maestra_dict, columna):
    maestro_keys = list(lista_maestra_dict.keys())
    df[columna] = df[columna].str.upper()
    # Obtiene los mejores matches para cada solicitante
    best_matches = [process.extractOne(solic, maestro_keys) for solic in df[columna]]
    
    # Extrae los nombres de las coincidencias
    best_match_names = [match[0] if match[1] > 80 else solic for solic, match in zip(df[columna], best_matches)]
    
    # En lugar de sobrescribir la columna original, crearemos una nueva columna con los nombres corregidos
    # Esta columna tendrá el mismo nombre que la columna original con el sufijo "Corregido"
    df[columna + ' Corregido'] = best_match_names
    return df



def generate_hash_id(*args):
    """Genera un HASH ID en base a una entrada."""
    combined_string = ''.join(map(str, args))
    return hashlib.sha256(combined_string.encode()).hexdigest()

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

def create_ZMM621_COMODIN_OC_HES_HEM(df_ZMM621_fechaAprobacion):
    """
    Crea el dataframe ZMM621 para el estado de HES/HEM, utilizando la columna 'COMODIN OC' como llave primaria.
    
    Parámetros:
    - df_ZMM621_fechaAprobacion (DataFrame): Se carga los datos directamente cargados del SAP.
    
    Retorna:
    - DataFrame: DataFrame procesado.
    """
    
    # Filtramos las Ordenes de compra para quedarnos solo con la más reciente según la fecha de HES/EM (Fecha de registro.1)
    df_ZMM621_HES_HEM = sort_and_remove_duplicates(df_ZMM621_fechaAprobacion, ['COMODIN OC', 'Fecha de registro.1'], 'COMODIN OC')

    # Utilizamos una operación vectorizada para crear la columna 'Estado HES/HEM'
    mask = pd.isna(df_ZMM621_HES_HEM['Fecha de registro.1'])
    df_ZMM621_HES_HEM['Estado HES/HEM'] = np.where(mask, "SIN HES/EM", "ACEPTADO")

    return df_ZMM621_HES_HEM

def create_ZMM621_Orden_unique(df_ZMM621_fechaAprobacion):
    """
    Crea el dataframe ZMM621 , usando la columna 'Orden de mantenimiento'como llave primary

    Parámetros:
    - df_ZMM621(DataFrame): Se carga los datos directamente cargados del SAP 

    Retorna:
    - DataFrame: DataFrame procesado.
    """
    # Convertir valores 'Unknown' a NaN
    df_ZMM621_fechaAprobacion['Orden de mantenimiento'] = df_ZMM621_fechaAprobacion['Orden de mantenimiento'].replace('Unknown', np.nan)
    
    # Convertir la columna a numérico
    df_ZMM621_fechaAprobacion['Orden de mantenimiento'] = pd.to_numeric(df_ZMM621_fechaAprobacion['Orden de mantenimiento'], errors='coerce').dropna()
    
    df_ZMM621_OMant = sort_and_remove_duplicates(df_ZMM621_fechaAprobacion,
                                                          ['Orden de mantenimiento', 'Fecha contable'],
                                                          'Orden de mantenimiento')
    rename_column(df_ZMM621_OMant, 'Orden de mantenimiento', 'Orden')
    
    return df_ZMM621_OMant

def create_ZMM621_COMODIN_OC_unique(df_ZMM621_fechaAprobacion):
    """
    Crea el dataframe ZMM621 , usando la columna 'COMODIN OC'como llave primary

    Parámetros:
    - df_ZMM621(DataFrame): Se carga los datos directamente cargados del SAP 

    Retorna:
    - DataFrame: DataFrame procesado.
    """
    df_ZMM621_OCompras = sort_and_remove_duplicates(df_ZMM621_fechaAprobacion, ['COMODIN OC', 'Fecha contable'],
                                                    'COMODIN OC')
    # Utiliza operaciones vectorizadas para crear la columna 'Estado factura'
    mask = pd.isna(df_ZMM621_OCompras['Fecha Doc. Fact.'])
    df_ZMM621_OCompras['Estado factura'] = "FACTURADO"  # Valor por defecto
    df_ZMM621_OCompras.loc[mask, 'Estado factura'] = "SIN FACTURA"  # Actualiza solo las filas que cumplen con la máscara
    
    return df_ZMM621_OCompras

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

class CoincidenciaBuscadorFinal:
    def __init__(self, dataset_entrada, dataset_busqueda, columna_verificacion=None):
        self.dataset_entrada = dataset_entrada
        
        # Si se proporciona una columna_verificacion, verifica si hay valores duplicados en esa columna
        if columna_verificacion:
            # Verificar si hay valores duplicados en la columna especificada
            duplicados = dataset_busqueda[dataset_busqueda.duplicated(subset=columna_verificacion, keep=False)]
            
            # Si hay duplicados, mostrar los valores duplicados y eliminarlos
            if not duplicados.empty:
                print("Hay valores duplicados en la columna:", columna_verificacion)
                print(duplicados[columna_verificacion].unique())
                dataset_busqueda = dataset_busqueda.drop_duplicates(subset=columna_verificacion, keep='first')
                
        self.dataset_busqueda = dataset_busqueda


    def buscar_coincidencia(self, columna_a_buscar, columna_busqueda, valor_coincidencia, 
                            nombre_columna_resultado="Resultado", nombre_columna_busqueda=None):
        """
       Busca coincidencias exactas entre columna_a_buscar (en dataset_entrada) y 
       columna_busqueda (en dataset_busqueda). Si encuentra una coincidencia, 
       asigna valor_coincidencia en la columna resultado.
       """
        # Si no se especifica un nombre para la columna de búsqueda en el resultado, 
        # se usa el mismo nombre que columna_busqueda
        # Si no se especifica un nombre para la columna de búsqueda en el resultado, se usa el mismo nombre que columna_busqueda
        if not nombre_columna_busqueda:
            nombre_columna_busqueda = columna_busqueda
        
        # Convertir las columnas a tipo string
        self.dataset_entrada[columna_a_buscar] = self.dataset_entrada[columna_a_buscar].astype(str)
        self.dataset_busqueda[columna_busqueda] = self.dataset_busqueda[columna_busqueda].astype(str)
        
        # Cambiamos el nombre de la columna de búsqueda para el merge
        df_busqueda_renombrado = self.dataset_busqueda.rename(columns={columna_busqueda: nombre_columna_busqueda})
        
        # Utilizamos un merge left para buscar coincidencias
        resultado = pd.merge(self.dataset_entrada, df_busqueda_renombrado[[nombre_columna_busqueda]], 
                             left_on=columna_a_buscar, right_on=nombre_columna_busqueda, 
                             how='left', indicator=True)
        
        # Si la columna '_merge' es 'both', entonces hubo coincidencia
        resultado[nombre_columna_resultado] = (resultado['_merge'] == 'both').astype(str).replace({'True': valor_coincidencia, 'False': ''})
        
        # Eliminamos las columnas extra
        resultado.drop(['_merge'], axis=1, inplace=True)
        return resultado
    
def inmovilizadosConverted(df_inmovilizados, df_criticos):
    """
    Procesa un DataFrame de acuerdo a las especificaciones dadas:
    - Elimina el nombre de las columnas y lo transforma en una fila.
    - Elimina las dos primeras filas.
    - Establece la tercera fila como encabezados.
    - Conserva solo las columnas deseadas.
    - Restablece el índice.
    - Busca coincidencias con df_criticos y etiqueta como "CRITICO" o "NO CRITICO".
    - Calcula días inmovilizados.
    - Calcula la deducción y el saldo.
    """
    
    # Mover los nombres de las columnas a una fila en el DataFrame
    df_inmovilizados.columns = range(df_inmovilizados.shape[1])
    df_inmovilizados = df_inmovilizados.reset_index(drop=True)

    # Eliminar las dos primeras filas
    df_inmovilizados = df_inmovilizados.iloc[1:]

    # Establecer la tercera fila (ahora la primera fila) como encabezados
    df_inmovilizados.columns = df_inmovilizados.iloc[0]
    df_inmovilizados = df_inmovilizados.iloc[1:]
    
    # Conservar solo las columnas deseadas
    desired_columns = ['Material', 'Descripcion', 'Valor stock', 'Moneda', 'Stock', 'AREA', 'PEDIDO POR', 
                       'RESPONSABLE', 'OBSERVACIONES', 'Und', 'Últ.entr.', ' Últ.mov.', 'Tipo de Repuesto']
    df_inmovilizados = df_inmovilizados[desired_columns]
    
    # Restablecer el índice
    df_inmovilizados.reset_index(drop=True, inplace=True)
    
    # Buscar coincidencias y etiquetar como "CRITICO" o "NO CRITICO"
    buscador = CoincidenciaBuscadorFinal(df_inmovilizados, df_criticos)
    df_inmovilizados = buscador.buscar_coincidencia("Material", "Código SAP.", "CRITICO", "Tipo de repuesto", None)
    df_inmovilizados["Tipo de repuesto"].replace("", "NO CRITICO", inplace=True)
    
    # Convertir 'Últ.mov.' a datetime y calcular días inmovilizados
    df_inmovilizados[' Últ.mov.'] = pd.to_datetime(df_inmovilizados[' Últ.mov.'])
    df_inmovilizados['Dias Inmovilizados'] = (pd.Timestamp.now() - df_inmovilizados[' Últ.mov.']).dt.days
    
    # Define la función que calcula la tasa basada en los días inmovilizados
    def get_tasa(days):
        if 180 <= days <= 360:
            return 0.5
        elif 361 <= days <= 720:
            return 0.6
        elif days > 720:
            return 1
        return 0
    
    # Calcular deducción y saldo
    mask_no_critico = df_inmovilizados["Tipo de Repuesto"] == "NO CRITICO"
    df_inmovilizados.loc[mask_no_critico, "Deducción"] = df_inmovilizados.loc[mask_no_critico, "Dias Inmovilizados"].apply(get_tasa) * df_inmovilizados.loc[mask_no_critico, "Valor stock"].astype(float)
    df_inmovilizados["SALDO"] = df_inmovilizados["Valor stock"].astype(float) - df_inmovilizados["Deducción"]
    
    # Etiquetar como 'inmovilizado' si los días inmovilizados son >= 180 y el tipo de repuesto es "NO CRITICO"
    condition = (df_inmovilizados["Dias Inmovilizados"] >= 180) & (df_inmovilizados["Tipo de repuesto"] == "NO CRITICO")
    df_inmovilizados["Estado Inmovilizado"] = np.where(condition, "inmovilizado", "")
    # Eliminar valores duplicados basados en todas las columnas
    df_inmovilizados.drop_duplicates(inplace=True)
    return df_inmovilizados

def vectorized_process_material(df, columns):
    """
    Procesa las columnas especificadas en un DataFrame para que sean consistentes y manejables.
    En particular, ajusta los tipos de datos de las columnas, convirtiendo la columna en un tipo de dato string.
    """
    
    for col in columns:
        is_nan = pd.isna(df[col])
        is_numeric = df[col].astype(str).str.isnumeric()
        
        df.loc[is_numeric, col] = df.loc[is_numeric, col].astype(int).astype(str)
        df.loc[is_nan, col] = np.nan

def set_column_dtypes(data, column_type_mapping):
    """
    Establece tipos de datos específicos para columnas en el DataFrame.

    Parámetros:
    - data (DataFrame): El DataFrame.
    - column_type_mapping (dict): Diccionario que mapea nombres de columnas a tipos de datos deseados.

    Retorna:
    - DataFrame con tipos de columna modificados.
    """
    default_na_values = {
        'float64': 0,
        'int64': 0,
        'datetime64[ns]': pd.Timestamp('1970-01-01'),
        'str': 'Unknown'
    }

    for column_name, data_type in column_type_mapping.items():
        if column_name in data.columns:
            try:
                default_value = default_na_values.get(data_type, "Unknown")
                data[column_name].fillna(default_value, inplace=True)
                data[column_name] = data[column_name].astype(data_type)
                #print(f"La columna {column_name} se ha convertido a {data_type}.")
            except Exception as e:
                print(f"No se pudo convertir la columna {column_name} a {data_type}. Excepción: {e}")
                if data_type == 'float64':
                    data[column_name] = data[column_name].astype(str)
    return data

def process_dataframes_for_join(df_ME5A,
                                df_ZMM621_fechaAprobacion,
                                df_IW38, 
                                df_ME2N_OC,
                                df_ZMB52,
                                df_MCBE,df_inmovilizados,
                                df_criticos
                                ):
    """Prepara DataFrames para las operaciones de join."""
    column_types = {
        'Fecha de solicitud': 'datetime64[ns]',
        'Fecha de reg. Factura': 'datetime64[ns]',
        'Fecha de registro.1': 'datetime64[ns]',
        'Fecha contable': 'datetime64[ns]',
        'Fecha de aprobación de la orden de compr': 'datetime64[ns]',
        'Fecha documento': 'datetime64[ns]',
        # Añade cualquier otra columna que necesites definir aquí
    }

    set_column_dtypes(df_ME5A, column_types)
    set_column_dtypes(df_ZMM621_fechaAprobacion, column_types)
    set_column_dtypes(df_IW38, column_types)
    set_column_dtypes(df_ME2N_OC, column_types)
    set_column_dtypes(df_ZMB52, column_types)
    set_column_dtypes(df_MCBE, column_types)
    
    
    #######################################
    column_name_mapping = {
    'SOLICITANTE': 'Solicitante',
    'Indicador de Liberación': 'Indicador liberación',
    'ESTRATÉGIA DE LIBERACIÓN':'Estrategia liberac.',
    'Numero de orden':'Orden de mantenimiento',
    
    }
    
    def standardize_columns_for_dataframe(df, column_mapping):
        df.rename(columns=column_mapping, inplace=True)
    
    standardize_columns_for_dataframe(df_ME5A, column_name_mapping)
    standardize_columns_for_dataframe(df_ZMM621_fechaAprobacion, column_name_mapping)
    standardize_columns_for_dataframe(df_IW38, column_name_mapping)
    standardize_columns_for_dataframe(df_ME2N_OC, column_name_mapping)
    standardize_columns_for_dataframe(df_ZMB52, column_name_mapping)
    standardize_columns_for_dataframe(df_MCBE, column_name_mapping)
    
    lista_maestra_dict = {
    "EMANCHEGOM": "JEF-MM03",
    "MLAGUNAR(G)": "JEF-GE01",
    "MLAGUNAR": "JEF-ME02",
    "YPANDIAP": "JEF-ME01",
    "CTICSER": "JEF-MG01",
    "JPACCOC": "JEF-EM01",
    "ARADOP": "JEF-PL01",
    "MMELGARN":"JEF-MG01",
    "MMAGOB":"JEF-MG01",
    "PPAREDEST":"",
    "JDELGADOCH":"",
    "GLUNAR":"",
    "331_TECCOMUN":"",
    }
    
    # Corrigiendo la columna por defecto 'Solicitante'

    corregir_solicitantes_vectorizado(df_ME5A, lista_maestra_dict,'Solicitante')
    corregir_solicitantes_vectorizado(df_ZMM621_fechaAprobacion,lista_maestra_dict,'Solicitante de la solicitud pedido')
    corregir_solicitantes_vectorizado(df_ME2N_OC,lista_maestra_dict,'Solicitante')
    
    df_ZMB52 = df_ZMB52.pivot_table(
        index=['Material'],
        values=['Valor libre util.', 'Libre utilización'],
        aggfunc='sum').reset_index()
    
    # Columnas a procesar
    cols_to_process = ['Material']
    vectorized_process_material(df_ZMB52, cols_to_process)
    vectorized_process_material(df_ME5A, cols_to_process)
    
    # Se crean versiones procesadas de ciertos DataFrames para usar en la fusión
    df_ZMM621_OCompras = create_ZMM621_COMODIN_OC_unique(df_ZMM621_fechaAprobacion)
    df_ZMM621_OMant = create_ZMM621_Orden_unique(df_ZMM621_fechaAprobacion)
    df_ZMM621_HES_HEM = create_ZMM621_COMODIN_OC_HES_HEM(df_ZMM621_fechaAprobacion)
    
    cols_to_process=['Orden']
    vectorized_process_material(df_ZMM621_OMant,cols_to_process)
    vectorized_process_material(df_IW38,cols_to_process)
    
    df_inmovilizados_converted = inmovilizadosConverted(df_inmovilizados,df_criticos)
    
    return df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_inmovilizados_converted, df_criticos,df_ZMM621_OCompras,df_ZMM621_OMant,df_ZMM621_HES_HEM