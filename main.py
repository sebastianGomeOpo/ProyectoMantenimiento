import streamlit as st
import pandas as pd
import data_processing as dp
from utilities.process_dataframes import process_MCBE, validate_and_create_comodin_columns

def process_uploaded_files(files):
    dfs = {}
    
    # Carga de DataFrames
    try:
        dfs["ME5A"] = pd.read_excel(files[0],engine='openpyxl')
        if dfs["ME5A"] is None:
            st.error("Error: El archivo ME5A no se cargó correctamente.")
            return
        dfs["ZMM621"] = pd.read_excel(files[1],engine='openpyxl')
        if dfs["ZMM621"] is None:
            st.error("Error: El archivo ZMM621 no se cargó correctamente.")
            return
        dfs["IW38"] = pd.read_excel(files[2],engine='openpyxl')
        if dfs["IW38"] is None:
            st.error("Error: El archivo IW38 no se cargó correctamente.")
            return
        dfs["ME2N"] = pd.read_excel(files[3],engine='openpyxl')
        if dfs["ME2N"] is None:
            st.error("Error: El archivo ME2N no se cargó correctamente.")
            return
        dfs["ZMB52"] = pd.read_excel(files[4],engine='openpyxl')
        if dfs["ZMB52"] is None:
            st.error("Error: El archivo ZMB52 no se cargó correctamente.")
            return
        dfs["MCBE"] = process_MCBE(pd.read_excel(files[5],engine='openpyxl'))
        if dfs["MCBE"] is None:
            st.error("Error: El archivo MCBE no se cargó correctamente.")
            return
        dfs["CRITICOS"] = pd.read_excel(files[6],engine='openpyxl')
        if dfs["CRITICOS"] is None:
            st.error("Error: El archivo CRITICOS no se cargó correctamente.")
            return
        dfs["INMOVILIZADOS"] = pd.read_excel(files[7],engine='openpyxl')
        if dfs["INMOVILIZADOS"] is None:
            st.error("Error: El archivo INMOVILIZADOS no se cargó correctamente.")
            return

        try:
            dfs["tipos_cambio"] = pd.read_excel(files[8],engine='openpyxl')
            if dfs["tipos_cambio"] is None:
                st.error("Error: Los archivos de tipos de cambio no se cargaron correctamente.")
                return
        except Exception as e:
            st.error(f"Error cargando y unificando los archivos de tipos de cambio: {e}")
            return

    except Exception as e:
        st.error(f"Error cargando los archivos: {e}")
        return
    
    keys = list(dfs.keys())
    # Procesamiento de DataFrames
    for key in keys:
        df = dfs[key]
        if df is None:
            st.error(f"Error: El DataFrame {key} es None.")
            return
        if key in ["ME5A", "ZMM621", "ME2N"]:
            try:
                dfs[f"{key}_ComodinCreated"], message = validate_and_create_comodin_columns(df, f"df_{key}")
                st.write(message)
            except Exception as e:
                st.error(f"Error validando y creando columnas para {key}: {e}")
                return
    
    try:
        result, processed_dataframes_dict  = dp.process_data(
            dfs["ME5A_ComodinCreated"], 
            dfs["ZMM621_ComodinCreated"], 
            dfs["IW38"], 
            dfs["ME2N_ComodinCreated"], 
            dfs["ZMB52"], 
            dfs["MCBE"], 
            dfs["CRITICOS"], 
            dfs["INMOVILIZADOS"], 
            dfs["tipos_cambio"]
        )

    except Exception as e:
        st.error(f"Error processing the data: {e}")
        return

    return result,processed_dataframes_dict 

def main():
    st.title("Aplicación de Procesamiento de Datos")


    # Inicialización del estado de sesión para el botón de procesamiento
    if 'processed' not in st.session_state:
        st.session_state.processed = False
    
    if 'downloading' not in st.session_state:
        st.session_state.downloading = False
        
    # Carga de archivos usando Streamlit
    files = [
        st.file_uploader("Subir archivo ME5A", type=["xlsx"]),
        st.file_uploader("Subir archivo ZMM621", type=["xlsx"]),
        st.file_uploader("Subir archivo IW38", type=["xlsx"]),
        st.file_uploader("Subir archivo ME2N", type=["xlsx"]),
        st.file_uploader("Subir archivo ZMB52", type=["xlsx"]),
        st.file_uploader("Subir archivo MCBE", type=["xlsx"]),       # Nuevo archivo
        st.file_uploader("Subir archivo CRITICOS", type=["xlsx"]),   # Nuevo archivo
        st.file_uploader("Subir archivo INMOVILIZADOS", type=["xlsx"]), # Nuevo archivo
        st.file_uploader("Subir archivo Tipos de cambio", type=["xlsx"]),
    ]
    
    if all(files) and not st.session_state.downloading:
        st.success("Todos los archivos han sido subidos correctamente.")
   # Botón de procesamiento   
    if st.button("Procesar archivos") or st.session_state.processed:
        st.session_state.downloading = False  # Reset downloading flag
        

        if all(files):
            try:
                st.write("Procesando...")
                result, processed_dataframes_dict = process_uploaded_files(files)
                
                if result is not None:
                    st.success("Procesamiento completado exitosamente.")
                    
                    # Guardar el archivo CSV para descarga
                    csv_filename = "reporte_procesado.csv"
                    result.to_csv(csv_filename, index=False)
                    
                    # Guardar el archivo Excel para descarga
                    excel_filename = "archivos_procesados.xlsx"
                    # Names of your datasets, modify this according to your actual names
                    with pd.ExcelWriter(excel_filename) as writer:
                        result.to_excel(writer, sheet_name='Result', index=False)
                
                        for sheet_name, df in processed_dataframes_dict.items():
                            df.to_excel(writer, sheet_name=sheet_name, index=False)

                    # Botón de descarga para el archivo Excel
                    with open(excel_filename, "rb") as f:
                        st.download_button(
                            label="Descargar archivos procesados",
                            data=f,
                            file_name="archivos_procesados.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    st.session_state.processed = True
                
            except Exception as e:
                st.error(f"Error procesando los datos: {e}")
        else:
            st.warning("Por favor, sube todos los archivos antes de procesar.")

if __name__ == "__main__":
    main()