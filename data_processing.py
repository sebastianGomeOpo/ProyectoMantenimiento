import pandas as pd
from utilities import process_dataframes as pd_util
from utilities import merge_dataframes as md_util
from utilities import refine_joined_data as rjd_util
from utilities import calculate_additional_columns as cac_util

import time
class Timer:
    def __init__(self, message):
        self.message = message
        
    def __enter__(self):
        self.start = time.time()
        
    def __exit__(self, *args):
        self.end = time.time()
        elapsed_time = self.end - self.start
        print(f"{self.message}: {elapsed_time:.2f} seconds")
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
# Función Principal de Procesamiento
# -------------------------
def process_data(df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_criticos, df_inmovilizados, df_tipos_cambio):
    # start_time = time.time()
    processed_dataframes = pd_util.process_dataframes_for_join(df_ME5A, df_ZMM621_fechaAprobacion, df_IW38, df_ME2N_OC, df_ZMB52, df_MCBE, df_inmovilizados, df_criticos)
    
    #-----------------------------------------
    # dataframe_labels = [
    #   "df_ME5A", "df_ZMM621_fechaAprobacion", "df_IW38", "df_ME2N_OC", 
    #   "df_ZMB52", "df_MCBE", "df_inmovilizados_converted", "df_ZMM621_fechaHES_HEM_converted"
    #]

    # Mostrar el orden de los DataFrames
    #for index, label in enumerate(dataframe_labels):
    #   print(f"{index}: {label}")

    #------------------------------------------
    # Cuando necesites acceder a un DataFrame específico, usa su índice
    df_ME5A_converted = processed_dataframes[0]
    df_ZMM621_fechaAprobacion_converted = processed_dataframes[1]
    df_IW38_converted = processed_dataframes[2]
    df_ME2N_OC_converted = processed_dataframes[3]
    df_ZMB52_converted = processed_dataframes[4]
    df_MCBE_converted = processed_dataframes[5]
    df_inmovilizados_converted = processed_dataframes[6]
    df_criticos_converted = processed_dataframes[7]
    # start_time = time.time()
    # Merge the dataframes
    joined_data = md_util.merge_dataframes(df_ME5A_converted, df_ZMM621_fechaAprobacion_converted, df_IW38_converted,
                                   df_ME2N_OC_converted, df_ZMB52_converted, df_MCBE_converted,
                                   df_tipos_cambio)
    # print("Time for merging dataframes:", time.time() - start_time, "seconds")
    
    # print(f"Rows after merge_dataframes:{joined_data.shape[0]}")
    # Refine the merged data
    #start_time = time.time()
    joined_data = rjd_util.refine_joined_data(joined_data)
    # print(f"Rows after refining joined data:{joined_data.shape[0]}")
    
    # Calculate additional columns
    # start_time = time.time()
    joined_data = cac_util.calculate_additional_columns(joined_data, df_tipos_cambio,df_inmovilizados_converted,df_criticos_converted)
    # print("Time for calculating additional columns:", time.time() - start_time, "seconds")
    # print(f"Rows after calculating additional columns:{joined_data.shape[0]}")
    column_order = [
        'COMODIN OC', 'COMODIN SOLPED', 'Ind.liberación', 'TIPO', 'Solicitante','Solicitante Corregido', 'Pto.tbjo.responsable',
        'Estado HES/HEM', 'Fecha de reg. Factura', 'Estado factura',
        'Fecha contable', 'Estado contable', 'Fecha de HES/EM','Material', 'N° Activo',
        'Cantidad solicitada', 'Unidad de medida', 'Solicitud de pedido','Pos.solicitud pedido','Cantidad de pedido',
        'Por entregar (cantidad)', 'Pedido','Posición', 'Indicador liberación', 'Orden', 'Condición de pago del pedido',
        'Valor net. Solped', 'Denominación de la ubicación técnica', 'Denominación de objeto técnico', 'Equipo',
        'Por entregar (STATUS)', 'Por entregar (valor)', 'Precio neto', 'Descripcion Material', 'Moneda',
        'Libre utilización', 'Indicador de borrado SOLPED', 'Año OC', 'Mes OC', 'Indicador de borrado Orden de Compra',
        'Fecha de SOLPED',
        'Fecha de OC', 'PENDIENTE DE LIBERACIÓN DE OC', 'Fecha de aprobación de la orden de compr',
        'DEMORA EN GENERAR OC (DIAS)', 'DEMORA EN LIBERACIONES DE OC', 'Proveedor/Centro suministrador',
        'Estado liberación', 'Estrategia liberac.', 'Tipo de Cambio', 'Precio Convertido Dolares',
        'TIPO COMPROMETIDO SUGERENCIA', 'Últ.mov.', 'Últ.cons.', 'Últ.salida','Costo compras por retirar']

    # Reorder the dataframe columns
    joined_data = joined_data[column_order]

    joined_data.drop(['COMODIN OC', 'COMODIN SOLPED', 'Ind.liberación'],axis=1,inplace=True)
    return joined_data
