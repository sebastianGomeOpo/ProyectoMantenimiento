import streamlit as st
import pandas as pd
from data_processing import *
import base64

def process_uploaded_files(files):
    dfs = {}
    
    # Carga de DataFrames
    try:
        dfs["ME5A"] = pd.read_excel(files[0])
        if dfs["ME5A"] is None:
            st.error("Error: El archivo ME5A no se cargó correctamente.")
            return
        dfs["ZMM621"] = pd.read_excel(files[1])
        if dfs["ZMM621"] is None:
            st.error("Error: El archivo ZMM621 no se cargó correctamente.")
            return
        dfs["IW38"] = pd.read_excel(files[2])
        if dfs["IW38"] is None:
            st.error("Error: El archivo IW38 no se cargó correctamente.")
            return
        dfs["ME2N"] = pd.read_excel(files[3])
        if dfs["ME2N"] is None:
            st.error("Error: El archivo ME2N no se cargó correctamente.")
            return
        dfs["ZMB52"] = pd.read_excel(files[4])
        if dfs["ZMB52"] is None:
            st.error("Error: El archivo ZMB52 no se cargó correctamente.")
            return
        dfs["MCBE"] = process_MCBE(pd.read_excel(files[5]))
        if dfs["MCBE"] is None:
            st.error("Error: El archivo MCBE no se cargó correctamente.")
            return
        dfs["CRITICOS"] = pd.read_excel(files[6])
        if dfs["CRITICOS"] is None:
            st.error("Error: El archivo CRITICOS no se cargó correctamente.")
            return
        dfs["INMOVILIZADOS"] = pd.read_excel(files[7])
        if dfs["INMOVILIZADOS"] is None:
            st.error("Error: El archivo INMOVILIZADOS no se cargó correctamente.")
            return

        try:
            dfs["tipos_cambio"] = dp.load_and_unify_dataframes(files[8], files[9])
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
                dfs[f"{key}_ComodinCreated"], message = dp.validate_and_create_comodin_columns(df, f"df_{key}")
                st.write(message)
            except Exception as e:
                st.error(f"Error validando y creando columnas para {key}: {e}")
                return
    
    try:
        result = dp.process_data(dfs["ME5A_ComodinCreated"], dfs["ZMM621_ComodinCreated"], dfs["IW38"], dfs["ME2N_ComodinCreated"], dfs["ZMB52"], dfs["MCBE"], dfs["CRITICOS"], dfs["INMOVILIZADOS"], dfs["tipos_cambio"])
    except Exception as e:
        st.error(f"Error procesando los datos: {e}")
        return
    
    return result

def main():
    st.title("Aplicación de Procesamiento de Datos")
    
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
        st.file_uploader("Subir archivo PEN a USD", type=["csv"]),
        st.file_uploader("Subir archivo EUR a USD", type=["csv"])
    ]
    
    if all(files):
        st.success("Todos los archivos han sido subidos correctamente.")
    
    # Botón de procesamiento
    if st.button("Procesar archivos"):
        if all(files):
            try:
                st.write("Procesando...")
                result = process_uploaded_files(files)
                if result is not None:
                    st.success("Procesamiento completado exitosamente.")
                    # Exportar resultado a Excel y crear enlace de descarga
                    csv = result.to_csv(index=False)
                    b64 = base64.b64encode(csv.encode()).decode()
                    href = f'<a href="data:file/csv;base64,{b64}" download="reporte_procesado.csv">Descargar archivo procesado</a>'
                    st.markdown(href, unsafe_allow_html=True)
                    
                # Aquí puedes agregar el botón de descarga si lo necesitas
            except Exception as e:
                st.error(f"Error procesando los datos: {e}")
        else:
            st.warning("Por favor, sube todos los archivos antes de procesar.")

if __name__ == "__main__":
    main()