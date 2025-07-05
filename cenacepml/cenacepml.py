import streamlit as st
import requests
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import warnings
import json
import xml.etree.ElementTree as ET
import os

# Suprimir advertencias de SSL (solo para desarrollo, no recomendado en producción)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# --- Constante para la ruta del logotipo ---
# Obtener el directorio actual del script
SCRIPT_DIR = os.path.dirname(__file__) if "__file__" in locals() else os.getcwd()

# Nombres de archivo probables para el logo, con extensiones comunes
SOLUMEX_LOGO_POSSIBLE_FILENAMES = [
    "PHOTO-2025-06-20-13-29-21.jpg",
    "PHOTO-2025-06-20-13-29-21.jpeg",
    "PHOTO-2025-06-20-13-29-21.png",
    "solumex_logo.png",
    "solumex_logo.jpg"
]
SOLUMEX_LOGO_PATH_FOUND = None

for fname in SOLUMEX_LOGO_POSSIBLE_FILENAMES:
    full_path = os.path.join(SCRIPT_DIR, fname)
    if os.path.exists(full_path):
        SOLUMEX_LOGO_PATH_FOUND = full_path
        break

# --- Funciones para obtener y procesar datos ---
class CenacePMLFetcher:
    def __init__(self, base_url="https://ws01.cenace.gob.mx:8082/SWPML/SIM/"):
        self.base_url = base_url

    def fetch_pml_data(self, sistema, proceso, lista_nodos, fecha_inicio, fecha_fin, formato="JSON"):
        try:
            start_date = datetime.strptime(fecha_inicio, "%Y-%m-%d")
            end_date = datetime.strptime(fecha_fin, "%Y-%m-%d")
        except ValueError:
            return None

        anio_ini = start_date.year
        mes_ini = f"{start_date.month:02d}"
        dia_ini = f"{start_date.day:02d}"
        anio_fin = end_date.year
        mes_fin = f"{end_date.month:02d}"
        dia_fin = f"{end_date.day:02d}"

        nodos_url_param = "-".join(lista_nodos)

        url = (
            f"{self.base_url}{sistema}/{proceso}/{nodos_url_param}/"
            f"{anio_ini}/{mes_ini}/{dia_ini}/"
            f"{anio_fin}/{mes_fin}/{dia_fin}/{formato}"
        )

        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()

            if formato.upper() == "JSON":
                return response.json()
            elif formato.upper() == "XML":
                return ET.fromstring(response.content)
            else:
                return None

        except requests.exceptions.HTTPError:
            return None
        except requests.exceptions.ConnectionError:
            return None
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.RequestException:
            return None
        except json.JSONDecodeError:
            return None
        except ET.ParseError:
            return None
        except Exception:
            return None

    def process_json_response(self, json_data):
        results = []
        reports_to_process = []

        if isinstance(json_data, dict):
            if "Reporte" in json_data:
                if isinstance(json_data["Reporte"], list):
                    reports_to_process = json_data["Reporte"]
                else:
                    reports_to_process = [json_data["Reporte"]]
            elif "nombre" in json_data and "Resultados" in json_data:
                reports_to_process = [json_data]
            else:
                return results
        elif isinstance(json_data, list):
            reports_to_process = json_data
        else:
            return results

        for report_item in reports_to_process:
            if not isinstance(report_item, dict):
                continue

            try:
                resultados_content = report_item.get("Resultados")
                all_nodes_from_report = []

                if isinstance(resultados_content, dict):
                    node_data_from_dict = resultados_content.get("Nodo")
                    if isinstance(node_data_from_dict, list):
                        all_nodes_from_report.extend(node_data_from_dict)
                    elif isinstance(node_data_from_dict, dict):
                        all_nodes_from_report.append(node_data_from_dict)
                elif isinstance(resultados_content, list):
                    all_nodes_from_report.extend(resultados_content)
                else:
                    continue

                for nodo_data in all_nodes_from_report:
                    if not isinstance(nodo_data, dict):
                        continue

                    clv_nodo = nodo_data.get("clv_nodo")
                    if clv_nodo:
                        valores_data = nodo_data.get("Valores")

                        if isinstance(valores_data, list):
                            for valor_item in valores_data:
                                if not isinstance(valor_item, dict):
                                    continue
                                try:
                                    data = {
                                        "clv_nodo": clv_nodo,
                                        "fecha": valor_item.get("fecha"),
                                        "hora": int(valor_item.get("hora")),
                                        "pml": float(valor_item.get("pml")),
                                        "pml_ene": float(valor_item.get("pml_ene")),
                                        "pml_per": float(valor_item.get("pml_per")),
                                        "pml_cng": float(valor_item.get("pml_cng"))
                                    }
                                    if all(v is not None for v in data.values()):
                                        results.append(data)
                                except (ValueError, TypeError):
                                    pass
                        else:
                            pass
                    else:
                        pass
            except Exception:
                pass
        return results


    def process_xml_response(self, xml_root):
        results = []
        if xml_root is not None and xml_root.tag == "Reporte":
            for nodo_element in xml_root.findall(".//Nodo"):
                clv_nodo = nodo_element.find("clv_nodo").text if nodo_element.find("clv_nodo") is not None else None
                if clv_nodo:
                    for valor_element in nodo_element.findall(".//Valor"):
                        try:
                            data = {
                                "clv_nodo": clv_nodo,
                                "fecha": valor_element.find("fecha").text if valor_element.find("fecha") is not None else None,
                                "hora": int(valor_element.find("hora").text) if valor_element.find("hora") is not None else None,
                                "pml": float(valor_element.find("pml").text) if valor_element.find("pml") is not None else None,
                                "pml_ene": float(valor_element.find("pml_ene").text) if valor_element.find("pml_ene") is not None else None,
                                "pml_per": float(valor_element.find("pml_per").text) if valor_element.find("pml_per") is not None else None,
                                "pml_cng": float(valor_element.find("pml_cng").text) if valor_element.find("pml_cng") is not None else None
                            }
                            if all(v is not None for v in data.values()):
                                results.append(data)
                        except (ValueError, TypeError):
                            pass
        return results

# --- Cargar el catálogo de nodos (ahora acepta objetos de archivo o rutas de string) ---
@st.cache_data
def load_nodos_catalogo(file_source):
    try:
        df = pd.DataFrame()
        file_extension = None

        if isinstance(file_source, str): # Si es una ruta de archivo (string)
            file_extension = os.path.splitext(file_source)[1].lower()
            if file_extension == '.csv':
                df = pd.read_csv(file_source)
            elif file_extension == '.xlsx':
                df = pd.read_excel(file_source)
            else:
                return pd.DataFrame()
        else: # Si es un objeto de archivo de Streamlit (UploadedFile)
            file_extension = os.path.splitext(file_source.name)[1].lower()
            if file_extension == '.csv':
                df = pd.read_csv(file_source)
            elif file_extension == '.xlsx':
                df = pd.read_excel(file_source)
            else:
                st.error("Formato de archivo subido no soportado. Por favor, sube un archivo CSV o XLSX.")
                return pd.DataFrame()

        original_columns = df.columns.tolist()
        cleaned_columns = [col.strip().replace(' ', '_').replace('(', '').replace(')', '') for col in original_columns]
        df.columns = cleaned_columns

        rename_map = {
            'CLAVE': 'CLAVE_NODO_P',
            'ENTIDAD_FEDERATIVA_INEGI': 'ESTADO',
            'MUNICIPIO_INEGI': 'MUNICIPIO',
            'SISTEMA': 'SISTEMA'
        }

        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        required_columns_after_rename = ['CLAVE_NODO_P', 'ESTADO', 'MUNICIPIO', 'SISTEMA']
        for col in required_columns_after_rename:
            if col not in df.columns:
                return pd.DataFrame()
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# --- Interfaz de Streamlit ---
st.set_page_config(layout="wide")

# Agregar logotipo en la parte superior central
if SOLUMEX_LOGO_PATH_FOUND:
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_left:
        st.image(SOLUMEX_LOGO_PATH_FOUND, width=150) # Ancho fijo en píxeles
else:
    st.warning(f"Logotipo no encontrado. Asegúrate de que el archivo de imagen '{SOLUMEX_LOGO_POSSIBLE_FILENAMES[0]}' (u otros nombres comunes) esté en la misma carpeta que el script.")


st.title("Consulta y Análisis de Precios Marginales Locales (PML) - CENACE")

st.markdown("""
Esta aplicación permite consultar los Precios Marginales Locales (PML) del CENACE
para nodos específicos, visualizar los datos y graficarlos.
""")

st.sidebar.header("⚙️ Configuración")

# Convertir el texto a un enlace y simplificar el mensaje del uploader
st.sidebar.markdown(
    """
    **Catálogo de Nodos P:**
    Puedes descargar el catálogo oficial de CENACE desde [aquí](https://www.cenace.gob.mx/Paginas/SIM/NodosP.aspx).
    """
)
uploaded_file = st.sidebar.file_uploader("Sube el archivo (CSV o XLSX) del catálogo", type=["csv", "xlsx"])


nodos_df = pd.DataFrame()

default_catalogo_path = "Catalogo Nodos P.csv"
try:
    if uploaded_file is not None:
        nodos_df = load_nodos_catalogo(uploaded_file)
        if nodos_df.empty:
            st.sidebar.warning("El catálogo subido está vacío o no es válido. Intentando cargar el predeterminado.")
            try:
                nodos_df = load_nodos_catalogo(default_catalogo_path)
            except FileNotFoundError:
                pass
    else:
        try:
            nodos_df = load_nodos_catalogo(default_catalogo_path)
        except FileNotFoundError:
            pass

except Exception:
    pass

if nodos_df.empty:
    st.success("<--- Cargar un catálogo de nodos P válido. Por favor, sube un archivo CSV o XLSX desde la barra lateral.")
    st.stop()

# --- SELECTORES DE FILTRO ---
estados = sorted(nodos_df['ESTADO'].unique().tolist()) if 'ESTADO' in nodos_df.columns else []
selected_estado = st.sidebar.selectbox("Seleccionar Estado", ["Todos los Estados"] + estados)

municipios = []
if selected_estado != "Todos los Estados" and 'MUNICIPIO' in nodos_df.columns:
    municipios = sorted(nodos_df[nodos_df['ESTADO'] == selected_estado]['MUNICIPIO'].unique().tolist())
selected_municipio = st.sidebar.selectbox("Seleccionar Municipio", ["Todos los Municipios"] + municipios)

nodos_filtrados_por_ubicacion = []
if selected_estado != "Todos los Estados" and selected_municipio != "Todos los Municipios" and 'CLAVE_NODO_P' in nodos_df.columns:
    nodos_filtrados_por_ubicacion = sorted(nodos_df[(nodos_df['ESTADO'] == selected_estado) & (nodos_df['MUNICIPIO'] == selected_municipio)]['CLAVE_NODO_P'].unique().tolist())
elif selected_estado != "Todos los Estados" and selected_municipio == "Todos los Municipios" and 'CLAVE_NODO_P' in nodos_df.columns:
    nodos_filtrados_por_ubicacion = sorted(nodos_df[nodos_df['ESTADO'] == selected_estado]['CLAVE_NODO_P'].unique().tolist())
elif selected_estado == "Todos los Estados" and 'CLAVE_NODO_P' in nodos_df.columns:
    nodos_filtrados_por_ubicacion = sorted(nodos_df['CLAVE_NODO_P'].unique().tolist())

selected_nodo_option_label = "Selecciona un Nodo Individual"
if selected_municipio != "Todos los Municipios":
    selected_nodo_option_label = f"Todos los Nodos del Municipio ({len(nodos_filtrados_por_ubicacion)})"
elif selected_estado != "Todos los Estados":
    selected_nodo_option_label = f"Todos los Nodos del Estado ({len(nodos_filtrados_por_ubicacion)})"
else:
    selected_nodo_option_label = f"Todos los Nodos (Total: {len(nodos_filtrados_por_ubicacion)})"

selected_nodo = st.sidebar.selectbox("Detalle de la Consulta", [selected_nodo_option_label] + nodos_filtrados_por_ubicacion)


nodos_a_consultar = []
consulta_nivel_individual_nodo = False
if selected_nodo == selected_nodo_option_label:
    nodos_a_consultar = nodos_filtrados_por_ubicacion
else:
    nodos_a_consultar = [selected_nodo]
    consulta_nivel_individual_nodo = True


if 'SISTEMA' in nodos_df.columns and not nodos_df['SISTEMA'].empty:
    sistemas_disponibles = sorted(nodos_df['SISTEMA'].unique().tolist())
else:
    sistemas_disponibles = ["SIN", "BCA", "BCS"]
selected_sistema = st.sidebar.selectbox("Seleccionar Sistema", sistemas_disponibles)

procesos = ["MDA", "MTR"]
selected_proceso = st.sidebar.selectbox("Seleccionar Proceso", procesos)

st.sidebar.subheader("Rango de Fechas")
fecha_actual = datetime.now()
if fecha_actual.day < 3:
    first_day_of_current_month = fecha_actual.replace(day=1)
    fecha_fin_default = first_day_of_current_month - pd.Timedelta(days=1)
else:
    fecha_fin_default = fecha_actual

fecha_inicio = st.sidebar.date_input("Fecha de Inicio", (fecha_fin_default - pd.Timedelta(days=6)).date())
fecha_fin = st.sidebar.date_input("Fecha de Fin", fecha_fin_default.date())

status_message_placeholder = st.empty()
progress_bar_placeholder = st.empty()

if st.sidebar.button("Obtener Datos PML"):
    if not nodos_a_consultar:
        st.warning("No hay nodos seleccionados para consultar. Por favor, ajusta los filtros.")
    elif selected_sistema == "" or selected_proceso == "":
        st.warning("Por favor, selecciona un Sistema y un Proceso.")
    elif fecha_inicio > fecha_fin:
        st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
    else:
        status_message_placeholder.info(f"Obteniendo datos para {len(nodos_a_consultar)} nodo(s) seleccionado(s)... Esto puede tardar.")
        progress_bar = progress_bar_placeholder.progress(0)
        all_pml_data = []
        fetcher = CenacePMLFetcher()

        for i, nodo in enumerate(nodos_a_consultar):
            raw_data = fetcher.fetch_pml_data(selected_sistema, selected_proceso, [nodo], fecha_inicio.strftime("%Y-%m-%d"), fecha_fin.strftime("%Y-%m-%d"), formato="JSON")

            if raw_data is not None:
                if isinstance(raw_data, dict) or isinstance(raw_data, list):
                    pml_data = fetcher.process_json_response(raw_data)
                    if pml_data:
                        all_pml_data.extend(pml_data)
                elif ET.iselement(raw_data):
                    pml_data = fetcher.process_xml_response(raw_data)
                    if pml_data:
                        all_pml_data.extend(pml_data)
            progress_bar.progress((i + 1) / len(nodos_a_consultar))

        status_message_placeholder.empty()
        progress_bar_placeholder.empty()

        if all_pml_data:
            pml_df = pd.DataFrame(all_pml_data)

            # --- Corrección para '24:00:00' y creación de 'fecha_hora' ---
            pml_df_processed = pml_df.copy()

            pml_df_processed['hora_int'] = pd.to_numeric(pml_df_processed['hora'], errors='coerce').fillna(0).astype(int)
            pml_df_processed['fecha_dt'] = pd.to_datetime(pml_df_processed['fecha'], errors='coerce')

            mask_hora_24 = pml_df_processed['hora_int'] == 24
            pml_df_processed.loc[mask_hora_24, 'hora_int'] = 0
            pml_df_processed.loc[mask_hora_24, 'fecha_dt'] += timedelta(days=1)

            pml_df_processed['hora_str_formatted'] = pml_df_processed['hora_int'].astype(str).str.zfill(2)
            pml_df_processed['fecha_hora'] = pd.to_datetime(
                pml_df_processed['fecha_dt'].dt.strftime('%Y-%m-%d') + ' ' + pml_df_processed['hora_str_formatted'] + ':00:00',
                format='%Y-%m-%d %H:%M:%S',
                errors='coerce'
            )
            pml_df_processed = pml_df_processed.dropna(subset=['fecha_hora'])

            pml_df_processed['pml'] = pd.to_numeric(pml_df_processed['pml'], errors='coerce')
            pml_df_processed = pml_df_processed.dropna(subset=['pml'])
            pml_df_processed = pml_df_processed.sort_values(by=['fecha_hora']).reset_index(drop=True)
            # --- Fin de la corrección ---

            # Unir con información de ubicación del catálogo de nodos
            pml_df_merged = pml_df_processed.merge(nodos_df[['CLAVE_NODO_P', 'ESTADO', 'MUNICIPIO']],
                                                   left_on='clv_nodo',
                                                   right_on='CLAVE_NODO_P',
                                                   how='left')
            pml_df_merged.drop(columns=['CLAVE_NODO_P'], inplace=True)


            # --- Lógica para mostrar promedio o individual ---
            if consulta_nivel_individual_nodo:
                st.subheader(f"Gráfica Interactiva de PML para el Nodo: {selected_nodo} ({selected_estado}, {selected_municipio})")

                chart = alt.Chart(pml_df_merged).mark_line(point=True).encode(
                    x=alt.X('fecha_hora', axis=alt.Axis(title='Fecha y Hora', format='%Y-%m-%d %H:%M')),
                    y=alt.Y('pml', title='Precio Marginal Local (PML)'),
                    tooltip=[
                        alt.Tooltip('fecha', title='Fecha Original API'),
                        alt.Tooltip('hora', title='Hora Original API'),
                        alt.Tooltip('fecha_hora', title='Timestamp Corregido'),
                        alt.Tooltip('pml', title='PML', format='$.2f'), # Formato de moneda
                        alt.Tooltip('pml_ene', title='PML_ENE', format='$.2f'), # Formato de moneda
                        alt.Tooltip('pml_per', title='PML_PER', format='$.2f'), # Formato de moneda
                        alt.Tooltip('pml_cng', title='PML_CNG', format='$.2f') # Formato de moneda
                    ]
                ).properties(
                    title=f"PML por Hora para {selected_nodo}"
                ).interactive()

                st.altair_chart(chart, use_container_width=True)

                st.subheader(f"Datos PML para el Nodo: {selected_nodo}")
                st.write("A continuación se muestran los datos obtenidos:")
                # Formatear la tabla directamente para PML con moneda
                st.dataframe(pml_df_merged.drop(columns=['fecha_dt', 'hora_int', 'hora_str_formatted']).style.format({
                    'pml': '${:.2f}',
                    'pml_ene': '${:.2f}',
                    'pml_per': '${:.2f}',
                    'pml_cng': '${:.2f}'
                }))

            else: # Se seleccionó "Todos los Nodos..." a nivel de estado, municipio o general
                st.subheader(f"Gráfica Interactiva de PML Promedio por Hora para {selected_nodo}")

                group_cols = ['fecha_hora']
                title_suffix = ""
                tooltip_cols = [
                    alt.Tooltip('fecha_hora', title='Timestamp', format='%Y-%m-%d %H:%M'),
                    alt.Tooltip('pml_promedio', title='PML Promedio', format='$.2f') # Formato de moneda
                ]

                if selected_municipio != "Todos los Municipios":
                    group_cols.extend(['ESTADO', 'MUNICIPIO'])
                    title_suffix = f"para {selected_municipio} ({selected_estado})"
                    tooltip_cols.append(alt.Tooltip('MUNICIPIO'))
                elif selected_estado != "Todos los Estados":
                    group_cols.append('ESTADO')
                    title_suffix = f"para {selected_estado}"
                    tooltip_cols.append(alt.Tooltip('ESTADO'))
                else:
                    title_suffix = "para todos los nodos"

                average_pml_df = pml_df_merged.groupby(group_cols)['pml'].mean().reset_index()
                average_pml_df.columns = group_cols + ['pml_promedio']

                chart_avg = alt.Chart(average_pml_df).mark_line(point=True).encode(
                    x=alt.X('fecha_hora', axis=alt.Axis(title='Fecha y Hora', format='%Y-%m-%d %H:%M')),
                    y=alt.Y('pml_promedio', title='PML Promedio'),
                    tooltip=tooltip_cols
                ).properties(
                    title=f"PML Promedio por Hora {title_suffix}"
                ).interactive()

                st.altair_chart(chart_avg, use_container_width=True)

                st.subheader("Datos PML Promedio por Hora:")
                # Formatear la tabla directamente para PML promedio con moneda
                st.dataframe(average_pml_df.style.format({
                    'pml_promedio': '${:.2f}'
                }))


            # --- Tabla de PML Promedio, Máximo y Mínimo por Estado (para el periodo completo) ---
            st.subheader("PML Resumen por Estado (Periodo Seleccionado)")
            if 'ESTADO' in pml_df_merged.columns:
                pml_summary_by_state = pml_df_merged.groupby('ESTADO')['pml'].agg(
                    PML_Promedio='mean',
                    PML_Maximo='max',
                    PML_Minimo='min'
                ).reset_index()
                pml_summary_by_state.columns = ['Estado', 'PML Promedio', 'PML Máximo', 'PML Mínimo']
                pml_summary_by_state = pml_summary_by_state.sort_values('PML Promedio', ascending=False).reset_index(drop=True)
                # Formato de moneda para la tabla resumen
                st.dataframe(pml_summary_by_state.style.format({
                    'PML Promedio': '${:.2f}',
                    'PML Máximo': '${:.2f}',
                    'PML Mínimo': '${:.2f}'
                }))
            else:
                st.warning("La columna 'ESTADO' no se encontró en los datos PML para generar el resumen por estado.")

            # --- Opciones de Descarga ---
            csv_export = pml_df_processed.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar datos PML brutos (CSV)",
                data=csv_export,
                file_name=f"pml_data_raw_{selected_nodo.replace(' ', '_')}_{fecha_inicio.strftime('%Y%m%d')}_to_{fecha_fin.strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
            if not consulta_nivel_individual_nodo and 'average_pml_df' in locals():
                csv_export_avg = average_pml_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar PML Promedio por Hora (CSV)",
                    data=csv_export_avg,
                    file_name=f"pml_promedio_hora_{selected_nodo.replace(' ', '_')}_{fecha_inicio.strftime('%Y%m%d')}_to_{fecha_fin.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )

        else:
            st.warning("No se encontraron datos PML para los parámetros seleccionados.")
    # Final de la lógica del botón "Obtener Datos PML"

st.sidebar.markdown("---")
st.sidebar.info("Desarrollado para la consulta de PML del CENACE.")