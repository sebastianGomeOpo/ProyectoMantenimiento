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
    processed_dataframes = pd_util.process_dataframes_for_join(df_ME5A, 
                                                               df_ZMM621_fechaAprobacion,
                                                               df_IW38,
                                                               df_ME2N_OC,
                                                               df_ZMB52, 
                                                               df_MCBE,
                                                               df_inmovilizados,
                                                               df_criticos)
    
    # Cuando necesites acceder a un DataFrame específico, usa su índice
    df_ME5A_converted = processed_dataframes[0]
    df_ZMM621_fechaAprobacion_converted = processed_dataframes[1]
    df_IW38_converted = processed_dataframes[2]
    df_ME2N_OC_converted = processed_dataframes[3]
    df_ZMB52_converted = processed_dataframes[4]
    df_MCBE_converted = processed_dataframes[5]
    df_inmovilizados_converted = processed_dataframes[6]
    df_criticos_converted = processed_dataframes[7]
    df_ZMM621_OCompras = processed_dataframes[8]
    df_ZMM621_OMant = processed_dataframes[9]
    df_ZMM621_HES_HEM = processed_dataframes[10]
    
    # Crear un diccionario con los dataframes procesados y sus nombres
    processed_dataframes_dict = {
        "ME5A": df_ME5A_converted,
        "ZMM621_fechaAprobacion": df_ZMM621_fechaAprobacion_converted,
        "IW38": df_IW38_converted,
        "ME2N_OC": df_ME2N_OC_converted,
        "ZMB52": df_ZMB52_converted,
        "MCBE": df_MCBE_converted,
        "inmovilizados": df_inmovilizados_converted,
        "criticos": df_criticos_converted,
        "ZMM621_OCompras": df_ZMM621_OCompras,
        "ZMM621_OMant": df_ZMM621_OMant,
        "ZMM621_HES_HEM": df_ZMM621_HES_HEM
    }
    
    processed_dataframes = [
        df_ME5A_converted,
        df_ZMM621_fechaAprobacion_converted,
        df_IW38_converted,
        df_ME2N_OC_converted,
        df_ZMB52_converted,
        df_MCBE_converted,
        df_inmovilizados_converted,
        df_criticos_converted,
        df_ZMM621_OCompras,
        df_ZMM621_OMant,
        df_ZMM621_HES_HEM
    ]
    
    joined_data = md_util.merge_dataframes(df_ME5A_converted, 
                                           df_ZMM621_fechaAprobacion_converted,
                                           df_IW38_converted,
                                           df_ME2N_OC_converted,
                                           df_ZMB52_converted, df_MCBE_converted,
                                           df_tipos_cambio,df_criticos_converted,
                                           df_inmovilizados_converted,
                                           df_ZMM621_OCompras,
                                           df_ZMM621_OMant,
                                           df_ZMM621_HES_HEM)
    
    joined_data = rjd_util.refine_joined_data(joined_data)
    
    joined_data = cac_util.calculate_additional_columns(joined_data, df_tipos_cambio,df_inmovilizados_converted,df_criticos_converted)
    
    column_order = [
        'COMODIN OC', 'COMODIN SOLPED', 'Ind.liberación', 'TIPO', 'Solicitante','Solicitante Corregido', 'Pto.tbjo.responsable',
        'Estado HES/HEM', 'Fecha de reg. Factura', 'Estado factura',
        'Fecha contable', 'Estado contable', 'Fecha de HES/EM','Material', 'Numero de activo',
        'Cantidad solicitada', 'Unidad de medida', 'Solicitud de pedido','Pos.solicitud pedido','Cantidad de pedido',
        'Por entregar (cantidad)', 'Pedido','Posición', 'Indicador liberación', 'Orden', 'Condición de pago del pedido',
        'Valor net. Solped', 'Denominación de la ubicación técnica', 'Denominación de objeto técnico', 'Equipo',
        'Por entregar (STATUS)', 'Por entregar (valor)', 'Precio neto', 'Descripcion Material', 'Moneda',
        'Libre utilización', 'Indicador de borrado SOLPED', 'Año OC', 'Mes OC', 'Indicador de borrado Orden de Compra',
        'Fecha de SOLPED',
        'Fecha de OC', 'PENDIENTE DE LIBERACIÓN DE OC', 'Fecha de aprobación de la orden de compr',
        'DEMORA EN GENERAR OC (DIAS)', 'DEMORA EN LIBERACIONES DE OC', 'Proveedor/Centro suministrador',
        'Estado liberación', 'Estrategia liberac.', 'Tipo de Cambio', 'Precio Convertido Dolares',
        'TIPO COMPROMETIDO SUGERENCIA', ' Últ.mov.', 'Últ.cons.', 'Últ.salida','Costo compras por retirar','Dias Inmovilizados','Estado Inmovilizado','Valor stock','Stock','Material Critico?']

    # Reorder the dataframe columns
    joined_data = joined_data[column_order]
    joined_data.drop(['Ind.liberación'],axis=1,inplace=True)
    return joined_data, processed_dataframes_dict