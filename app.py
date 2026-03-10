import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import time
from dotenv import load_dotenv

import helios_master 

def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["ACCESS_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Introduce la clave de acceso a Helios OS", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Introduce la clave de acceso a Helios OS", type="password", on_change=password_entered, key="password")
        st.error("😕 Contraseña incorrecta")
        return False
    else:
        return True

if not check_password():
    st.stop()  

load_dotenv()
st.set_page_config(page_title="Helios OS", page_icon="☀️", layout="wide")

@st.cache_resource
def conectar_bd():
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            creds = Credentials.from_service_account_file("credenciales.json", scopes=scopes)


        cliente = gspread.authorize(creds)
        spreadsheet_id = st.secrets.get("SPREADSHEET_ID", os.getenv("SPREADSHEET_ID"))
        return cliente.open_by_key(spreadsheet_id).sheet1
        
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

sheet = conectar_bd()

st.sidebar.title("🎯 Misión de Búsqueda")

mision_busqueda = st.sidebar.text_input("1. ¿A quién buscamos?", "instaladoras aire acondicionado Murcia")
propuesta_valor = st.sidebar.text_area("2. ¿Qué les vamos a ofrecer/vender?", "Un software de IA para automatizar la captación de clientes y ahorrar 10h semanales.")

if st.sidebar.button("🚀 Lanzar Orquestador"):
    if sheet:
        with st.spinner(f"Helios trabajando en: {mision_busqueda}..."):
            helios_master.orquestador(mision_busqueda, propuesta_valor)
            st.cache_resource.clear()
            st.sidebar.success("¡Misión completada!")
            st.rerun()
    else:
        st.sidebar.error("Sin conexión al Excel.")

st.sidebar.divider()
st.sidebar.info("Modo Human-in-the-loop activado. Los correos NO se enviarán hasta que tú los apruebes.")

st.sidebar.divider()
st.sidebar.subheader("📜 Actividad en vivo")
if os.path.exists("helios.log"):
    with open("helios.log", "r", encoding="utf-8") as f:
        lineas = f.readlines()
        st.sidebar.code("".join(lineas[-20:]), language="text")

st.title("☀️ Helios OS | Centro de Mando B2B")

if sheet:
    try:
        datos_crudos = sheet.get_all_values()
        if len(datos_crudos) > 1:
            cabeceras = datos_crudos[0]
            filas = datos_crudos[1:]
            
            df_total = pd.DataFrame(filas, columns=cabeceras)
            df_total = df_total.loc[:, df_total.columns != '']
            
            df = df_total[~df_total['Cualificado'].str.contains('NO|Descartado', case=False, na=False)]
            
            if df.empty:
                st.info("No hay prospectos cualificados para mostrar. ¡Lanza una nueva misión en la izquierda!")
                st.stop()
            
            col_m1, col_m2, col_m3 = st.columns(3)
            with col_m1:
                st.metric("Total Prospectos Activos", len(df))
            with col_m2:
                cualificados = len(df[df['Cualificado'].str.upper() == 'SI'])
                st.metric("Cualificados ✅", cualificados)
            with col_m3:
                enviados = len(df[df['Enviado'].str.upper() == 'SI'])
                st.metric("Emails en Bandeja 📩", enviados)

            st.divider()

            st.subheader("🕵️ Detalle y Auditoría de Prospecto")
            empresa_lista = df['Nombre'].tolist()
            empresa_seleccionada = st.selectbox("Selecciona una empresa para revisar su informe:", empresa_lista)
            
            row = df[df['Nombre'] == empresa_seleccionada].iloc[0]
            
            st.markdown(f"### {empresa_seleccionada}")
            st.write(f"🌐 **Web:** {row['Web']}")
            st.write(f"📧 **Contacto:** {row['Email Contacto']}")
            st.info(f"🧠 **Análisis de la IA:** {row['Resumen']}")
            
            with st.expander("📝 Editar y Ver Borrador de Email", expanded=True):
                asunto_editado = st.text_input("Asunto:", value=row['Asunto'])
                cuerpo_editado = st.text_area("Cuerpo del mensaje:", value=row['Cuerpo'], height=250)
            
            st.write("") 
            estado_envio = str(row['Enviado']).strip().upper()
            
            if estado_envio != 'SI':
                if not row['Email Contacto'] or "info@" in str(row['Email Contacto']):
                    st.warning("⚠️ Cuidado: Email genérico o no encontrado.")
                    
                if st.button(f"✅ Aprobar y Enviar correo a {empresa_seleccionada}", type="primary"):
                    with st.spinner("Guardando cambios y conectando con Gmail..."):
                        exito = helios_master.enviar_correo_manual(
                            empresa_seleccionada, 
                            nuevo_asunto=asunto_editado, 
                            nuevo_cuerpo=cuerpo_editado
                        )
                        if exito:
                            st.success("¡Enviado con éxito! 🚀")
                            st.cache_resource.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Hubo un error al enviar el correo.")
            else:
                st.success("✅ Este correo ya fue enviado en pruebas anteriores. ¡Selecciona otra empresa!")

            st.write("") 
            st.subheader("🥷 Conexión en LinkedIn")
            
            mensaje_li = str(row.get('Mensaje LinkedIn', ''))
            url_li = str(row.get('URL LinkedIn', ''))
            
            if mensaje_li and mensaje_li.lower() != "nan" and mensaje_li.strip() != "":
                st.info("✉️ Nota de conexión lista para copiar y pegar (máx 300 caracteres):")
                st.code(mensaje_li, language="text")
                
                resumen_texto = str(row['Resumen'])
                nombre_buscar = empresa_seleccionada
                if "[DATOS NINJA]" in resumen_texto:
                    try:
                        linea_ninja = [l for l in resumen_texto.split('\n') if "[DATOS NINJA]" in l][0]
                        nombre_buscar = linea_ninja.split(":")[1].split("|")[0].strip()
                    except: pass
                
                if url_li and "linkedin.com" in url_li.lower():
                    st.link_button(f"🎯 Ir al perfil exacto de {nombre_buscar}", url_li, type="primary")
                else:
                    url_linkedin = f"https://www.linkedin.com/search/results/people/?keywords={nombre_buscar.replace(' ', '%20')}%20{empresa_seleccionada.replace(' ', '%20')}"
                    st.link_button(f"🔍 Buscar a {nombre_buscar} en LinkedIn", url_linkedin)
            else:
                st.warning("El Ninja no encontró a un directivo claro o falta generar el mensaje.")

            st.divider()
            if st.button(f"🗑️ Descartar a {empresa_seleccionada}", help="Marca como NO cualificado para quitarlo de tu vista"):
                filas = sheet.get_all_values()
                for idx, f in enumerate(filas[1:], start=2):
                    if f[0] == empresa_seleccionada:
                        sheet.update_cell(idx, 3, "NO (Descartado)")
                        st.cache_resource.clear()
                        st.warning(f"{empresa_seleccionada} descartado.")
                        time.sleep(1)
                        st.rerun()

            st.divider()
            st.subheader("📊 Base de Datos CRM")
            st.dataframe(df, width="stretch")

        else:
            st.info("El CRM está vacío.")
    except Exception as e:
        st.error(f"Error visualizando datos: {e}")
