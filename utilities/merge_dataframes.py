import pandas as pd
from utilities.process_dataframes import CoincidenciaBuscadorFinal

def check_null_values(df, key):
    """Verifica si hay valores nulos en una columna específica de un DataFrame.
    
    Parámetros:
    - df (DataFrame): DataFrame a verificar.
    - key (str): Nombre de la columna para verificar los valores nulos.
    """
    null_count = df[key].isnull().sum()
    if null_count > 0:
        print(f"Precauciob: {null_count} valores nulos encontrados en la columna{key}")


def check_data_types(df1, df2, key):
    """Verifica que una columna específica exista en dos DataFrames y compara los tipos de datos.
    
    Parámetros:
    - df1, df2 (DataFrame): DataFrames a verificar.
    - key (str): Nombre de la columna para verificar.
    """
    if key not in df1.columns or key not in df2.columns:
        print(f"La columna clave '{key}' no se encuentra en uno de los DataFrames.")
        return False
    
    if df1[key].dtype != df2[key].dtype:
        print(f"Error: Los tipos de datos para la columna clave '{key}' no coinciden entre los DataFrames. "
              f"Tipo de datos en df1: {df1[key].dtype}, "
              f"Tipo de datos en df2: {df2[key].dtype}")
        return False
    
    return True
        
def check_columns_existence(df, columns):
    """Verifica si un conjunto de columnas existen en un DataFrame y lanza una excepción si alguna no se encuentra.
    
    Parámetros:
    - df (DataFrame): DataFrame a verificar.
    - columns (list): Lista de nombres de columnas para verificar.
    """
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        print(f"Missing columns in DataFrame: {missing_cols}")
        raise KeyError(f"Columns {missing_cols} not found in DataFrame!")
        
def check_left_join_assumptions(df_target, df_source, key):
    """Verifica varios supuestos antes de realizar un left join, como la existencia de la columna clave y tipos de datos compatibles.
    
    Parámetros:
    - df_target, df_source (DataFrame): DataFrames a verificar.
    - key (str): Nombre de la columna clave.
    
    Retorna:
    - bool: True si todos los supuestos se cumplen, de lo contrario False.
    """
    # Verifica que la columna clave exista en ambos DataFrames
    if key not in df_target.columns or key not in df_source.columns:
        print(f"Error: La columna clave '{key}' no se encuentra en uno de los DataFrames. "
              f"Columnas en df_target: {df_target.columns.tolist()}, "
              f"Columnas en df_source: {df_source.columns.tolist()}")
        return False
        
    # Verifica que los tipos de datos de la columna clave coincidan
    target_key_dtype = df_target[key].dtype
    source_key_dtype = df_source[key].dtype
    if target_key_dtype != source_key_dtype:
        print(f"Error: Los tipos de datos para la columna clave '{key}' no coinciden entre los DataFrames. "
              f"Tipo de datos en el DataFrame de destino: {target_key_dtype}, "
              f"Tipo de datos en el DataFrame fuente: {source_key_dtype}")
        return False

    # (Opcional) Verifica que la columna clave no tenga duplicados en el DataFrame fuente
    if df_source[key].duplicated().any():
        print(f"Advertencia: La columna clave '{key}' tiene valores duplicados en el DataFrame fuente. "
              "Esto podría llevar a resultados inesperados en el left join.")
        return False
    return True

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





def merge_dataframes(df_ME5A,
                     df_ZMM621_fechaAprobacion,
                     df_IW38, df_ME2N_OC, df_ZMB52,
                     df_MCBE,
                     df_tipos_cambio,
                     df_criticos_converted,
                     df_inmovilizados_converted,
                     df_ZMM621_OCompras,
                     df_ZMM621_OMant,
                     df_ZMM621_HES_HEM):
    """
   Fusiona múltiples DataFrames basándose en una lógica definida.

   Parámetros:
   - df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_tipos_cambio (DataFrames): Los DataFrames que se fusionarán.

   Retorna:
   - DataFrame: Resultado de la fusión de DataFrames.
   """
   # Se inicia con un DataFrame base usando ciertas columnas de df_ME5A
    joined_data = df_ME5A[['COMODIN SOLPED', 'COMODIN OC']]

    joined_data = left_join(joined_data, df_ZMM621_OMant, 'COMODIN OC', ['Orden'])
    
    # Se definen las operaciones de fusión (unión izquierda) que se realizarán en orden
    left_join_operations = [
        (df_ME5A, 'COMODIN SOLPED',
         ['Material', 'Pedido', 'Solicitud de pedido', 'Pos.solicitud pedido', 'Solicitante','Solicitante Corregido', 'Indicador de borrado',
          'Indicador liberación', 'Fecha de solicitud', 'Unidad de medida', 'Cantidad solicitada', 'Texto breve']),
        (df_ZMM621_OCompras, 'COMODIN OC',
         ['Estado factura', 'Fecha Doc. Fact.', 'Fecha de registro.1', 'Fecha contable',
          'Condición de pago del pedido', 'Fecha de aprobación de la orden de compr', 'Valor net. Solped',
          'Numero de activo']),
        (df_IW38, 'Orden',
         ['Pto.tbjo.responsable', 'Denominación de la ubicación técnica', 'Denominación de objeto técnico', 'Equipo']),
        (df_ME2N_OC, 'COMODIN OC',
         ['Proveedor/Centro suministrador','Posición', 'Estado liberación', 'Indicador de borrado', 'Fecha documento',
          'Por entregar (cantidad)', 'Cantidad de pedido', 'Precio neto', 'Moneda', 'Por entregar (valor)',
          'Ind.liberación', 'Estrategia liberac.']),
        (df_ZMB52, 'Material', ['Libre utilización']),
        (df_MCBE, 'Material', ['Últ.salida', 'Últ.cons.', ' Últ.mov.']),
        (df_ZMM621_HES_HEM, 'COMODIN OC', ['Estado HES/HEM']),
        (df_inmovilizados_converted,'Material',['Dias Inmovilizados','Estado Inmovilizado','Valor stock','Stock'])
    ]
    
    for df, key, columns in left_join_operations:
    # Verificar asunciones y realizar la unión
        if check_left_join_assumptions(joined_data, df, key):
            # Realizar comprobaciones adicionales
            check_columns_existence(df, [key] + columns)
            if not check_data_types(joined_data, df, key):
                print(f"Falló la unión de DataFrames con la columna clave '{key}' debido a tipos de datos incompatibles.")
                continue
            check_null_values(joined_data, key)
            
            # Realizar la unión
            joined_data = left_join(joined_data, df, key, columns)
        else:
            print(f"Falló la unión de DataFrames con la columna clave '{key}'. Revise los mensajes de error anteriores.")
    buscadorCriticos = CoincidenciaBuscadorFinal(joined_data, df_criticos_converted)
    joined_data = buscadorCriticos.buscar_coincidencia('Material','Código SAP.','Critico', 
                                                        'Material Critico?')
    return joined_data