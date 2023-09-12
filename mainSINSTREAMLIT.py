import pandas as pd
from data_processing import *

def process_uploaded_files(files):
    dfs = {}
    
    # Load DataFrames
    dfs["ME5A"] = pd.read_excel(files[0])
    dfs["ZMM621"] = pd.read_excel(files[1])
    dfs["IW38"] = pd.read_excel(files[2])
    dfs["ME2N"] = pd.read_excel(files[3])
    dfs["ZMB52"] = pd.read_excel(files[4])
    dfs["MCBE"] = process_MCBE(pd.read_excel(files[5]))
    dfs["criticos"] = pd.read_excel(files[6])  # Asumiendo que esta es la correcta. Puede cambiar
    dfs["inmovilizados"] = pd.read_excel(files[7])  # Asumiendo que esta es la correcta. Puede cambiar
    dfs["tipos_cambio"] = load_and_unify_dataframes(files[8], files[9])
    
    # Create a list of keys to iterate over
    keys = list(dfs.keys())
    
    # Process DataFrames
    for key in keys:
        df = dfs[key]
        if key in ["ME5A", "ZMM621", "ME2N"]:
            dfs[f"{key}_ComodinCreated"], _ = validate_and_create_comodin_columns(df, f"df_{key}")
    
    result = process_data(dfs["ME5A_ComodinCreated"], dfs["ZMM621_ComodinCreated"], dfs["IW38"], dfs["ME2N_ComodinCreated"], dfs["ZMB52"], dfs["MCBE"], dfs["criticos"], dfs["inmovilizados"], dfs["tipos_cambio"])
    return result

def main(files):
    result = process_uploaded_files(files)
    # Guardar el resultado en un archivo Excel
    output_path = "resultado.xlsx"
    result.to_excel(output_path, index=False)
    print(f"El archivo se ha guardado en {output_path}")

if __name__ == "__main__":
    # List of file paths for testing purposes
    files = [
        "../ME5A SOLPEDS.xlsx",
        "../ZMM621 FECHA APROBACION.xlsx",
        "../IW38.xlsx",
        "../ME2N OC.xlsx",
        "../ZMB52 STOCK.xlsx",
        "../MCBE.xlsx",
        "../CRITICOS.xlsx",
        "../INMOVILIZADOS.xlsx",  
        "../PEN to USD.csv",
        "../USD to EUR .csv"
    ]
    main(files)