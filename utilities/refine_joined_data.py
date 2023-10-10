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

def refine_joined_data(joined_data):
    """Refine the joined data by renaming columns and filling missing values."""
    rename_mappings = {
        'Fecha de solicitud': 'Fecha de SOLPED',
        'Indicador de borrado_x': 'Indicador de borrado SOLPED',
        'Indicador de borrado_y': 'Indicador de borrado Orden de Compra',
        'Texto breve': 'Descripcion Material',
        'Fecha documento': 'Fecha de OC',
        'Fecha Doc. Fact.':'Fecha de reg. Factura',
        'Fecha de registro.1':'Fecha de HES/EM'
        
        
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