import pandas as pd
from data_processing import process_data
from utilities.process_dataframes import process_MCBE, validate_and_create_comodin_columns
import time

class Timer:
    def __init__(self, message):
        self.message = message
        
    def __enter__(self):
        self.start = time.time()
        return self
        
    def __exit__(self, *args):
        self.end = time.time()
        elapsed_time = self.end - self.start
        print(f"{self.message}: {elapsed_time:.2f} seconds")

def process_uploaded_files(files):
    dfs = {}
    
    # Load DataFrames
    with Timer("Loading data"):
        dfs["ME5A"] = pd.read_excel(files[0])
        dfs["ZMM621"] = pd.read_excel(files[1])
        dfs["IW38"] = pd.read_excel(files[2])
        dfs["ME2N"] = pd.read_excel(files[3])
        dfs["ZMB52"] = pd.read_excel(files[4])
        dfs["MCBE"] = process_MCBE(pd.read_excel(files[5]))
        dfs["criticos"] = pd.read_excel(files[6])
        dfs["inmovilizados"] = pd.read_excel(files[7])
        dfs["tipos_cambio"] = pd.read_excel(files[8])

    # Process DataFrames
    with Timer("Processing data"):
        keys = list(dfs.keys())
        for key in keys:
            df = dfs[key]
            if key in ["ME5A", "ZMM621", "ME2N"]:
                dfs[f"{key}_ComodinCreated"], _ = validate_and_create_comodin_columns(df, f"df_{key}")
        result, processed_dataframes = process_data(dfs["ME5A_ComodinCreated"], dfs["ZMM621_ComodinCreated"], dfs["IW38"], dfs["ME2N_ComodinCreated"], dfs["ZMB52"], dfs["MCBE"], dfs["criticos"], dfs["inmovilizados"], dfs["tipos_cambio"])
    
    return result, processed_dataframes

def main(files):
    result, processed_dataframes_dict = process_uploaded_files(files)
    
    output_path = "resultado.xlsx"
    with pd.ExcelWriter(output_path) as writer:
        result.to_excel(writer, sheet_name='Result', index=False)
        
        for sheet_name, df in processed_dataframes_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"El archivo se ha guardado en {output_path}")


if __name__ == "__main__":
    files = [
        "../Agosto/ME5A SOLPEDS.xlsx",
        "../Agosto/ZMM621 FECHA APROBACION.xlsx",
        "../Agosto/IW38.xlsx",
        "../Agosto/ME2N OC.xlsx",
        "../Agosto/ZMB52 STOCK.xlsx",
        "../Agosto/MCBE.xlsx",
        "../Agosto/CRITICOS.xlsx",
        "../Agosto/INMOVILIZADOS.xlsx",  
        "../Agosto/Tasas de cambio.xlsx",
    ]
    main(files)