import pandas as pd 
import numpy as np 
from utilities.process_dataframes import CoincidenciaBuscadorFinal


def vectorized_calcular_estado_contable(df):
    """Vectorized version of calcular_estado_contable."""
    conditions = [
        df['Pedido'].isna(),
        df['Fecha contable'].isna()
    ]
    choices = ['SIN OC', 'CON OC NO CONTAB']

    return np.select(conditions, choices, default='CONTABILIZADO')

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
        mask = df['Estrategia liberac.'] == strategy
        status[mask] = handler(df.loc[mask, 'Estado liberación'])

    return status

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

def vectorized_calculate_days_difference(df):
    """Vectorized version of calculate_days_difference."""
    
    # Convertir las columnas de fechas a datetime
    # Fecha de elaboracion de compra
    df['Fecha de OC'] = pd.to_datetime(df['Fecha de OC'], errors='coerce')
    # Fecha de liberacion de la orden 
    df['Fecha de aprobación de la orden de compr'] = pd.to_datetime(df['Fecha de aprobación de la orden de compr'], errors='coerce')
    
    # Definir la columna de resultado
    df['DEMORA EN LIBERACIONES DE OC'] = None
    
    # Definir la máscara
    mask_sin_oc = (df['PENDIENTE DE LIBERACIÓN DE OC'] == "SIN OC")&(df['TIPO COMPROMETIDO SUGERENCIA'].isin(["SERV. POR FINALIZAR", "COMPRA POR LLEGAR"])) & (df['Indicador de borrado Orden de Compra'] == "")
    mask_con_oc = df['PENDIENTE DE LIBERACIÓN DE OC'].isin(['JEFE L', 'SIN OC', np.nan])&(df['TIPO COMPROMETIDO SUGERENCIA'].isin(["SERV. POR FINALIZAR", "COMPRA POR LLEGAR"])) & (df['Indicador de borrado Orden de Compra'] == "")
    
    # Calcular la diferencia de días basado en la máscara y las columnas de fechas
    df.loc[mask_sin_oc, 'DEMORA EN LIBERACIONES DE OC'] = (pd.Timestamp.today() - df.loc[mask_sin_oc, 'Fecha de OC']).dt.days
    df.loc[mask_con_oc, 'DEMORA EN LIBERACIONES DE OC'] = (df.loc[mask_con_oc, 'Fecha de aprobación de la orden de compr'] - df.loc[mask_con_oc, 'Fecha de OC']).dt.days
    
    return df  # Devuelve el DataFrame modificado completo


def vectorized_calculate_date_difference(df):
    # print("Entrando a vectorized_calculate_date_difference...")
    # print(f"Número de filas al inicio: {len(df)}")

    df['Fecha de OC'] = pd.to_datetime(df['Fecha de OC'], errors='coerce')
    df['Fecha de SOLPED'] = pd.to_datetime(df['Fecha de SOLPED'], errors='coerce')

    df['DEMORA EN GENERAR OC (DIAS)'] = None

    mask = (df['TIPO COMPROMETIDO SUGERENCIA'].isin(["SERV. POR FINALIZAR", "COMPRA POR LLEGAR"])) & (df['Indicador de borrado Orden de Compra'] == "")

    # print(f"Número de filas que cumplen con la máscara: {mask.sum()}")

    df.loc[mask, 'DEMORA EN GENERAR OC (DIAS)'] = np.where(
        pd.isnull(df.loc[mask, 'Fecha de OC']) & pd.isnull(df.loc[mask, 'Fecha de SOLPED']), "",
        np.where(pd.isnull(df.loc[mask, 'Fecha de OC']),
                 (pd.Timestamp.today() - df.loc[mask, 'Fecha de SOLPED']).dt.days,
                 (df.loc[mask, 'Fecha de OC'] - df.loc[mask, 'Fecha de SOLPED']).dt.days)
    )

    # print(f"Número de filas al final: {len(df)}")
    return df  # Devuelve el DataFrame modificado completo

def vectorized_get_tipo_cambio(df):
    """Vectorized version of get_tipo_cambio."""
    return np.where(df['Moneda'] == "PEN", df['Tipo_Cambio_PEN'],
                    np.where(df['Moneda'] == "EUR", df['Tipo_Cambio_EUR'], 1))

def vectorized_convertir_moneda(df):
    """Vectorized version of convertir_moneda."""
    return df['Precio neto'] / df['Tipo de Cambio']

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
    df['Costo compras por retirar'] = np.where(df['TIPO COMPROMETIDO SUGERENCIA']=='COMPRA POR RETIRAR',df['Precio Unitario'] * df['Libre utilización'],'')
    return df

def calculate_additional_columns(joined_data, df_tipos_cambio,df_inmovilizados_converted,df_criticos):
    """Calculate and add new columns based on the provided logic."""
    
    # start_time = time.time()
    
    # Estado contable
    joined_data['Estado contable'] = vectorized_calcular_estado_contable(joined_data)
    # print("Time for 'Estado contable':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Indicador de borrado SOLPED
    joined_data['Indicador de borrado SOLPED'] = np.where(joined_data['Indicador de borrado SOLPED'] == 'True',
                                                          'SOLPED BORRADA ', '')
    # print("Time for 'Indicador de borrado SOLPED':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Indicador de borrado Orden de Compra
    joined_data['Indicador de borrado Orden de Compra'] = np.where(
        joined_data['Indicador de borrado Orden de Compra'] == 'L', 'OC.BORRADA', '')
    # print("Time for 'Indicador de borrado Orden de Compra':", time.time() - start_time, "seconds")
    # start_time = time.time()


    joined_data['TIPO'] = np.where(joined_data['Material'].isna() | joined_data['Material'].isin(['', 'nan']),
                                   'SERVICIO', 'COMPRA')
    # print("Time for 'TIPO':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Por entregar (STATUS)
    mask = joined_data['Por entregar (cantidad)'].isna() | joined_data['Por entregar (cantidad)'].isin(['', 'nan']) | (
                joined_data['Por entregar (cantidad)'] > 0)
    joined_data['Por entregar (STATUS)'] = np.where(mask, 'PENDIENTE', 'CONCLUIDO')
    # print("Time for 'Por entregar (STATUS)':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # PENDIENTE DE LIBERACIÓN DE OC
    joined_data['PENDIENTE DE LIBERACIÓN DE OC'] = vectorized_calculate_status(joined_data)
    # print("Time for 'PENDIENTE DE LIBERACIÓN DE OC':", time.time() - start_time, "seconds")
    # start_time = time.time()

    joined_data['TIPO COMPROMETIDO SUGERENCIA'] = vectorized_tipoCromprometido(joined_data)
    # print("Time for 'TIPO COMPROMETIDO SUGERENCIA':", time.time() - start_time, "seconds")
    # start_time = time.time()
    
    # DEMORA EN GENERAR OC 
    # vectorized_calculate_date_difference
    joined_data = vectorized_calculate_date_difference(joined_data)
    # print("Time for 'vectorized_calculate_date_difference':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # DEMORA EN LIBERACIONES DE OC
    joined_data = vectorized_calculate_days_difference(joined_data)
    # print("Time for 'DEMORA EN LIBERACIONES DE OC':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Año OC
    joined_data['Año OC'] = pd.to_datetime(joined_data['Fecha de OC']).dt.year
    # print("Time for 'Año OC':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Mes OC
    joined_data['Mes OC'] = pd.to_datetime(joined_data['Fecha de OC']).dt.month
    # print("Time for 'Mes OC':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Merging with df_tipos_cambio
    joined_data = pd.merge(joined_data, df_tipos_cambio, left_on=['Año OC', 'Mes OC'], right_on=['Año', 'Mes'],
                           how='left')
    # print("Time for merging with df_tipos_cambio:", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Tipo de Cambio
    joined_data['Tipo de Cambio'] = vectorized_get_tipo_cambio(joined_data)
    # print("Time for 'Tipo de Cambio':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Converting Precio neto
    joined_data['Precio neto'] = pd.to_numeric(joined_data['Precio neto'], errors='coerce')
    # print("Time for converting 'Precio neto':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Converting Tipo de Cambio
    joined_data['Tipo de Cambio'] = pd.to_numeric(joined_data['Tipo de Cambio'], errors='coerce')
    # print("Time for converting 'Tipo de Cambio':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Precio Convertido Dolares
    joined_data['Precio Convertido Dolares'] = vectorized_convertir_moneda(joined_data)
    # print("Time for 'Precio Convertido Dolares':", time.time() - start_time, "seconds")
    # start_time = time.time()

    # Dropping columns
    joined_data = joined_data.drop(columns=['Tipo_Cambio_PEN', 'Tipo_Cambio_EUR', 'Año', 'Mes'])
    # print("Time for dropping columns:", time.time() - start_time, "seconds")
    
    joined_data = costoComprasPorRetirar(joined_data)
    
    # Reemplazar NaNs en la columna 'Material' de ambos DataFrames
    joined_data['Material'].fillna("Unknown", inplace=True)
    df_criticos['Código SAP.'].fillna("Unknown",inplace = True)
    
    buscadorCriticos = CoincidenciaBuscadorFinal(joined_data, df_criticos)
    joined_data = buscadorCriticos.buscar_coincidencia('Material','Código SAP.','Critico', 
                                                        'Material Critico?')
    
    
    df_inmovilizados_converted['Material'].fillna("Unknown", inplace=True)
    
    # Verificación de duplicados en df_inmovilizados_converted antes del merge
    total_rows = df_inmovilizados_converted.shape[0]
    unique_material_values = df_inmovilizados_converted['Material'].nunique()
    if total_rows != unique_material_values:
        print(f"Hay {total_rows - unique_material_values} valores duplicados en la columna 'Material' de df_inmovilizados_converted.")
            #df_inmovilizados_converted.drop_duplicates(subset='Material', inplace=True)
            # print("Valores duplicados eliminados.")
    else:
        print("No hay valores duplicados en la columna 'Material' de df_inmovilizados_converted.")
    # print(f"Rows after final left_join:{joined_data.shape[0]}")
    
    # Realizar el merge
    joined_data = joined_data.merge(df_inmovilizados_converted[['Material', 'Estado Inmovilizado', 'Dias Inmovilizados']], 
                                    on='Material', 
                                    how='left')
    return joined_data