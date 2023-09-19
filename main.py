import streamlit as st
import pandas as pd
import data_processing as dp
from utilities.process_dataframes import process_MCBE, load_and_unify_dataframes, validate_and_create_comodin_columns
from google.oauth2 import service_account
from gsheetsdb import connect
import gspread

# Este es el objeto de conexión que usarás para interactuar con Google Sheets.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
    ],
)
conn = connect(credentials=credentials)

@st.cache(ttl=600)
def run_query(query):
    """Ejecuta la consulta SQL en Google Sheets y devuelve un DataFrame."""
    rows = conn.execute(query, headers=1)
    return pd.DataFrame(rows.fetchall())

def test_google_sheets_connection(credentials):
    try:
        # Establece la conexión con Google Sheets
        gc = gspread.service_account_from_dict(credentials)
        sh = gc.open_by_url(st.secrets["private_gsheets_url"])
        worksheet = sh.get_worksheet(0)
        # Lee algunos datos
        data = worksheet.get_all_records()
        st.write(data)
        st.success("Conexión exitosa.")
    except Exception as e:
        st.error(f"Error: {str(e)}")

if st.button("Test Connection"):
    test_google_sheets_connection(st.secrets["gcp_service_account"])

def save_to_google_sheet(df, credentials, sheet_name="Sheet1"):
    """
    Guarda un DataFrame de pandas en Google Sheets.
    """
    try:
        # Establece la conexión con Google Sheets
        gc = gspread.service_account_from_dict(credentials)
        sh = gc.open_by_url(st.secrets["private_gsheets_url"])
        worksheet = sh.worksheet(sheet_name)
        # Si la hoja no está en blanco, la limpia
        worksheet.clear()
        # Actualiza la hoja con el DataFrame
        worksheet.insert_rows(df.values.tolist(), row=1)
        # Actualiza los encabezados
        worksheet.insert_row(df.columns.tolist(), index=1)
        st.success("Datos guardados exitosamente en Google Sheets.")
    except Exception as e:
        st.error(f"Error guardando los datos en Google Sheets: {str(e)}")
        
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
            dfs["tipos_cambio"] = load_and_unify_dataframes(files[8], files[9])
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
        result = dp.process_data(dfs["ME5A_ComodinCreated"], dfs["ZMM621_ComodinCreated"], dfs["IW38"], dfs["ME2N_ComodinCreated"], dfs["ZMB52"], dfs["MCBE"], dfs["CRITICOS"], dfs["INMOVILIZADOS"], dfs["tipos_cambio"])
    except Exception as e:
        st.error(f"Error procesando los datos: {e}")
        return
    
    return result

def main():
    st.title("Aplicación de Procesamiento de Datos")
    
    # Inicialización del estado de sesión para el botón de procesamiento
    if 'processed' not in st.session_state:
        st.session_state.processed = False
        
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
        st.file_uploader("Subir archivo PEN a USD", type=["xlsx"]),
        st.file_uploader("Subir archivo EUR a USD", type=["xlsx"])
    ]
    
    if all(files):
        st.success("Todos los archivos han sido subidos correctamente.")
    
    # Botón de procesamiento
    if st.button("Procesar archivos") or st.session_state.processed:
        if all(files):
            try:
                st.write("Procesando...")
                result = process_uploaded_files(files)
                if result is not None:
                    st.success("Procesamiento completado exitosamente.")
                    
                    # Guarda el resultado en Google Sheets
                    save_to_google_sheet(result, st.secrets["gcp_service_account"])
                    
                    # Botón de descarga
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label="Descargar archivo procesado",
                        data=csv,
                        file_name="reporte_procesado.csv",
                        mime="text/csv"
                    )
                # Aquí puedes agregar el botón de descarga si lo necesitas
            except Exception as e:
                st.error(f"Error procesando los datos: {e}")
        else:
            st.warning("Por favor, sube todos los archivos antes de procesar.")

if __name__ == "__main__":
    main()