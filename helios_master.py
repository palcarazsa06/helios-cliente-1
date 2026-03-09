import os
import time
import smtplib
import requests
import io
import PyPDF2
import gspread
import re
import random
import streamlit as st
from urllib.parse import urljoin, urlparse
from email.message import EmailMessage
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor

os.system("playwright install chromium")

# ==========================================
# 📜 SISTEMA DE LOGS (Consola Web)
# ==========================================
def log_web(mensaje):
    with open("helios.log", "a", encoding="utf-8") as f:
        f.write(f"{mensaje}\n")
    print(mensaje)

open("helios.log", "w").close() 
log_web("🚀 INICIANDO EL ORQUESTADOR HELIOS (Versión Text-Only)...")

load_dotenv()

api_gemini = st.secrets["GOOGLE_API_KEY"] if "GOOGLE_API_KEY" in st.secrets else os.getenv("GOOGLE_API_KEY")

llm_flash = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)
llm_creativo = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.5)

# 💡 FIX 1: Faltaba definir los permisos (scope) que necesita Google
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# --- NUEVA CONEXIÓN PARA LA NUBE ---
def conectar_google_sheets_nube():
    # Intentamos leer las credenciales desde los Secrets de Streamlit
    creds_dict = st.secrets["gcp_service_account"]
    
    # Creamos las credenciales directamente desde el diccionario (sin archivo .json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

log_web("1. ☁️ Conectando a la base de datos en la nube...")

# 💡 FIX 2: Inicializamos 'sheet' vacía para que no dé NameError si la conexión falla
sheet = None 

try:
    # Intenta primero el modo nube, si falla (porque estás en local), usa el archivo
    if "gcp_service_account" in st.secrets:
        cliente = conectar_google_sheets_nube()
    else:
        # Tu código antiguo para cuando trabajes en tu ordenador
        creds = ServiceAccountCredentials.from_json_keyfile_name('credenciales.json', scope)
        cliente = gspread.authorize(creds)
        
    sheet = cliente.open_by_key("1w6ug2YFj1wpMFNwCgS2sqewUmG_4m9RIMe2x2w3Bkm4").sheet1
except Exception as e:
    log_web(f"❌ Error crítico de conexión: {e}")

# 💡 FIX 3: Solo intentamos poner cabeceras si la conexión fue exitosa
if sheet:
    cabeceras = ["Nombre", "Web", "Cualificado", "Resumen", "Asunto", "Cuerpo", "Enviado", "Email Contacto", "Prompt Imagen", "Mensaje LinkedIn", "URL LinkedIn"]
    if not sheet.row_values(1):
        sheet.append_row(cabeceras)

# ==========================================
# 🕵️ FASE 1: RECOLECCIÓN (DUCKDUCKGO PURO)
# ==========================================
def fase_recoleccion(query_usuario):
    log_web(f"\n--- FASE 1: BÚSQUEDA EN INTERNET REAL: {query_usuario} ---")
    
    try:
        # Importamos la librería pura, sin intermediarios
        from duckduckgo_search import DDGS
        
        resultados_reales = ""
        # Buscamos de verdad en internet
        with DDGS() as ddgs:
            busqueda = list(ddgs.text(f"empresas {query_usuario} contacto web", max_results=10))
            for r in busqueda:
                resultados_reales += f"Título: {r['title']}\nWeb: {r['href']}\n\n"
        
        # Le damos los textos reales a la IA
        prompt = f"""
        Aquí tienes resultados reales de búsqueda de internet sobre '{query_usuario}':
        {resultados_reales}
        
        Extrae el Nombre de la empresa y su URL principal.
        REGLA 1: Solo extrae empresas que aparezcan arriba. NO INVENTES NADA.
        REGLA 2: Devuelve SOLO Nombre||URL (una empresa por línea). Máximo 5.
        """
        
        respuesta = llm_flash.invoke(prompt)
        texto = respuesta.content if hasattr(respuesta, 'content') else str(respuesta)
        
        filas_existentes = sheet.get_all_values()
        nombres_existentes = [fila[0].lower().strip() for fila in filas_existentes[1:] if len(fila) > 0]
        
        nuevas = 0
        for linea in texto.split('\n'):
            linea = linea.strip()
            if "||" in linea:
                partes = linea.split("||")
                nombre, web = partes[0].strip(), partes[1].strip()
                web = web.strip('.') 
                
                if nombre.lower() not in nombres_existentes and "http" in web:
                    sheet.append_row([nombre, web, "", "", "", "", "", "", "", "", ""])
                    nuevas += 1
                    
        log_web(f"✅ Se han añadido {nuevas} empresas 100% REALES al CRM.")
    except Exception as e:
        log_web(f"❌ Error en la recolección: {e}")

# ==========================================
# 🔬 FASE 2: CUALIFICACIÓN Y EXTRACCIÓN
# ==========================================
def extraer_texto_pdf(url_pdf):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        respuesta = requests.get(url_pdf, headers=headers, timeout=10)
        archivo_pdf = io.BytesIO(respuesta.content)
        lector = PyPDF2.PdfReader(archivo_pdf)
        texto_pdf = ""
        for i in range(min(3, len(lector.pages))):
            texto_pdf += lector.pages[i].extract_text() + "\n"
        return texto_pdf
    except Exception as e: return ""

def extraer_con_playwright(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=20000)
            texto_web = page.inner_text("body")
            
            enlaces = page.locator("a").element_handles()
            link_pdf = None
            for enlace in enlaces:
                href = enlace.get_attribute("href")
                if href and ".pdf" in href.lower():
                    link_pdf = urljoin(url, href)
                    break
            browser.close()
            texto_pdf = extraer_texto_pdf(link_pdf) if link_pdf else ""
            return f"TEXTO WEB:\n{texto_web}\n\nPOSIBLES PROYECTOS (PDF):\n{texto_pdf}"[:6000]
    except Exception as e: return f"Error de acceso: {e}"

def fase_cualificacion(fila, index, query_usuario, propuesta_valor):
    log_web(f"  🔍 Auditando web de {fila[0]}...")
    contexto = extraer_con_playwright(fila[1])
    
    # 💡 FIX: El prompt ahora evalúa si son buenos clientes para LO QUE VENDES
    prompt = f"""Actúa como auditor B2B. Texto web extraído: {contexto}
    Buscamos empresas con este perfil: '{query_usuario}'.
    Nuestro objetivo es ofrecerles esto: '{propuesta_valor}'.
    
    ¿La empresa '{fila[0]}' es un buen cliente potencial para esta oferta? Responde EXACTAMENTE:
    CUALIFICADO: [SI/NO]
    RESUMEN: [Máximo 30 palabras justificando por qué les serviría nuestra oferta]"""
    
    res = llm_flash.invoke([HumanMessage(content=prompt)]).content
    c, r = "NO", "Sin datos"
    for l in res.split('\n'):
        if "CUALIFICADO:" in l: c = l.split(":")[1].strip()
        if "RESUMEN:" in l: r = l.split(":")[1].strip()
    sheet.update_cell(index, 3, c)
    sheet.update_cell(index, 4, r)

# ==========================================
# 🎯 FASE 2.5: HUNTER & LINKEDIN
# ==========================================
def buscar_email_directivo(fila, index_fila):
    nombre, web = fila[0], fila[1]
    log_web(f"  🎯 Buscando email de contacto para {nombre}...")
    try: dominio = urlparse(web).netloc.replace('www.', '')
    except: dominio = ""
    if not dominio:
        sheet.update_cell(index_fila, 8, "No encontrado")
        return
    url = f"https://api.hunter.io/v2/domain-search?domain={dominio}&api_key={os.getenv('HUNTER_API_KEY')}"
    try:
        datos = requests.get(url).json()
        emails = datos.get('data', {}).get('emails', [])
        email_encontrado = emails[0]['value'] if emails else f"info@{dominio}"
        if emails: log_web(f"    🎯 ¡Blanco fijado!: {email_encontrado}")
        sheet.update_cell(index_fila, 8, email_encontrado)
    except Exception as e:
        sheet.update_cell(index_fila, 8, f"info@{dominio}")

# ==========================================
# 🥷 FASE 2.5: EL NINJA DE LINKEDIN (NIVEL DIOS - FIX DUCKDUCKGO PURO)
# ==========================================
def investigar_linkedin_directivo(fila, index_fila):
    nombre_empresa, resumen_actual = fila[0], fila[3]
    log_web(f"  🥷 Modo Ninja: X-Ray Search para {nombre_empresa}...")
    
    # 💡 FIX 1: Retraso aleatorio para evitar bloqueos
    time.sleep(random.uniform(2.0, 5.0))
    
    try:
        # 🔥 ELIMINAMOS LANGCHAIN AQUÍ Y USAMOS DDGS DIRECTO 🔥
        from duckduckgo_search import DDGS
        query = f'"{nombre_empresa}" (CEO OR Fundador OR Director OR Operaciones) site:linkedin.com/in/'
        
        resultados = ""
        try:
            with DDGS() as ddgs:
                busqueda = list(ddgs.text(query, max_results=4))
                for r in busqueda:
                    resultados += f"Perfil: {r['title']} - URL: {r['href']} - Info: {r['body']}\n"
        except Exception:
            resultados = "" # Si DDG falla, pasamos vacío para que active el Plan B
        
        # 💡 FIX 2: Prompt a prueba de fallos. SIEMPRE genera mensaje.
        prompt = f"""
        Analiza estos resultados de búsqueda sobre la empresa '{nombre_empresa}': 
        {resultados}
        
        Busca un directivo real. Si NO lo encuentras, inventa un mensaje genérico.
        
        Responde EXACTAMENTE con este formato de 4 líneas (sin asteriscos):
        NOMBRE: [Su Nombre, o NADA]
        CARGO: [Su Cargo, o Responsable]
        URL: [El enlace exacto de linkedin.com/in/..., o NADA]
        MENSAJE: [Nota de invitación de LinkedIn de MÁXIMO 250 caracteres. Si hay nombre, dirígete a él. Si no, dirígete al equipo de {nombre_empresa}]
        """
        
        # Invocamos a la IA
        respuesta = llm_flash.invoke(prompt)
        texto = respuesta.content if hasattr(respuesta, 'content') else str(respuesta)
        
        # 💡 FIX 3: Extracción robusta. Ignora negritas (**), asteriscos y espacios extra
        n_ninja = re.search(r"NOMBRE:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE)
        c_ninja = re.search(r"CARGO:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE)
        u_ninja = re.search(r"URL:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE)
        m_ninja = re.search(r"MENSAJE:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE)

        n_val = n_ninja.group(1).replace('*', '').strip() if n_ninja else ""
        c_val = c_ninja.group(1).replace('*', '').strip() if c_ninja else ""
        u_val = u_ninja.group(1).replace('*', '').strip() if u_ninja else ""
        m_val = m_ninja.group(1).replace('*', '').strip() if m_ninja else ""

        if n_val.upper() == "NADA": n_val = ""
        if u_val.upper() == "NADA": u_val = ""

        # Si encontramos a la persona, actualizamos el resumen para el email
        if n_val:
            datos_ninja = f"{n_val} | {c_val}"
            log_web(f"    🎯 ¡Perfil localizado!: {n_val}")
            nuevo_resumen = f"{resumen_actual}\n\n[DATOS NINJA]: {datos_ninja}\nINSTRUCCIÓN EXTRA: Empieza dirigiéndote a {n_val}, sobre su perfil de {c_val}."
            sheet.update_cell(index_fila, 4, nuevo_resumen)
        else:
            log_web(f"    ⚠️ Ninja ciego, pero se generó mensaje genérico de red.")
        
        # Limpieza final de la URL
        u_val = u_val.replace('<', '').replace('>', '').replace('"', '').replace("'", "")
        if u_val and "linkedin.com" in u_val and not u_val.startswith("http"):
            u_val = "https://" + u_val
            
        # Seguro final por si el modelo falla al dar el mensaje
        if not m_val:
            m_val = f"Hola, me encantaría conectar con el equipo de {nombre_empresa} para compartir sinergias. ¡Un saludo!"

        sheet.update_cell(index_fila, 10, m_val)
        sheet.update_cell(index_fila, 11, u_val)
        
    except Exception as e: 
        log_web(f"    ❌ Error Ninja: {e}")
        # Si explota todo, dejamos un mensaje genérico para que la web NO desaparezca
        sheet.update_cell(index_fila, 10, f"Hola, me encantaría conectar con el equipo de {nombre_empresa} para explorar sinergias. ¡Saludos!")

# ==========================================
# ✍️ FASE 3: REDACCIÓN DEL CORREO 
# ==========================================
def fase_redaccion(fila, index_fila, propuesta_valor):
    nombre, resumen = fila[0], fila[3]
    log_web(f"  ✍️ Redactando email para {nombre}...")
    
    # 💡 FIX: El copywriter ahora sabe exactamente qué tiene que vender
    prompt = f"""
    Eres un experto copywriter B2B. Tu cliente objetivo es la empresa: {nombre}. 
    Contexto de la empresa: {resumen}
    
    Lo que queremos venderles/ofrecerles es lo siguiente: {propuesta_valor}
    
    Escribe un correo corto y persuasivo de puerta fría ofreciendo nuestra solución. Ve al grano, sin saludos robóticos.
    
    Formato EXACTO de respuesta:
    ASUNTO: [Asunto corto que genere curiosidad]
    CUERPO: [Cuerpo del correo]
    """
    
    try:
        texto = llm_creativo.invoke([HumanMessage(content=prompt)]).content
        if "ASUNTO:" in texto and "CUERPO:" in texto:
            partes_cuerpo = texto.split("CUERPO:")
            asunto = partes_cuerpo[0].replace("ASUNTO:", "").strip().replace('\n', '')
            cuerpo = partes_cuerpo[1].strip()
            
            firma = "\n\n---\nTu Nombre\nTu Cargo | Tu Empresa\n📞 +34 600 000 000 | 🌐 tuweb.com"
            cuerpo = cuerpo + firma
            
            sheet.update_cell(index_fila, 5, asunto)
            sheet.update_cell(index_fila, 6, cuerpo)
            log_web("    ✅ Textos guardados.")
        else:
            log_web("    ⚠️ La IA no respetó el formato exacto.")
    except Exception as e:
        log_web(f"    ❌ Error en redacción: {e}")

# ==========================================
# 📩 FASE 4: EL CARTERO
# ==========================================
def fase_envio(fila, index_fila, servidor):
    nombre, asunto, cuerpo = fila[0], fila[4], fila[5]
    asunto_limpio = str(asunto).strip().replace('\n', '').replace('\r', '')
    log_web(f"  📩 Enviando correo a {nombre}...")
    msg = EmailMessage()
    msg.set_content(cuerpo)
    msg['Subject'] = asunto_limpio
    msg['From'] = os.getenv("GMAIL_USER")
    msg['To'] = str(fila[7]).strip() # 🎯 Ahora sí, se envía al prospecto real
    servidor.send_message(msg)
    sheet.update_cell(index_fila, 7, "SI")

def enviar_correo_manual(nombre_empresa, nuevo_asunto=None, nuevo_cuerpo=None):
    log_web(f"\n🚀 Lanzando envío manual aprobado para: {nombre_empresa}")
    try:
        filas = sheet.get_all_values()
        for index, fila in enumerate(filas[1:], start=2):
            if fila[0] == nombre_empresa:
                if fila[6].upper() != "SI":
                    if nuevo_asunto: sheet.update_cell(index, 5, nuevo_asunto); fila[4] = nuevo_asunto
                    if nuevo_cuerpo: sheet.update_cell(index, 6, nuevo_cuerpo); fila[5] = nuevo_cuerpo
                    servidor_smtp = smtplib.SMTP('smtp.gmail.com', 587)
                    servidor_smtp.starttls()
                    servidor_smtp.login(os.getenv("GMAIL_USER"), os.getenv("GMAIL_PASS"))
                    fase_envio(fila, index, servidor_smtp)
                    servidor_smtp.quit()
                    return True
        return False
    except Exception as e: return False

# ==========================================
# 🧠 PROCESAMIENTO DE UNA FILA ÚNICA
# ==========================================
def procesar_prospecto_individual(datos_proceso):
    # 💡 FIX: Desempaquetamos la nueva variable propuesta_valor
    index, fila, query_usuario, propuesta_valor = datos_proceso
    
    while len(fila) < 11: fila.append("")
    
    try:
        if fila[2] == "" or "ERROR" in fila[2].upper():
            # 💡 FIX: Le pasamos la propuesta a la cualificación
            fase_cualificacion(fila, index, query_usuario, propuesta_valor)
            fila = sheet.row_values(index) 
        
        if fila[2].upper() != "SI":
            return f"Fin: {fila[0]} (No cualificado)"

        while len(fila) < 11: fila.append("")
        if fila[7] == "":
            buscar_email_directivo(fila, index)
            fila = sheet.row_values(index)
            
        while len(fila) < 11: fila.append("")
        if "[DATOS NINJA" not in fila[3]:
            investigar_linkedin_directivo(fila, index)
            fila = sheet.row_values(index)

        while len(fila) < 11: fila.append("")
        if fila[4] == "":
            # 💡 FIX: Le pasamos la propuesta a la redacción
            fase_redaccion(fila, index, propuesta_valor)
            fila = sheet.row_values(index)

        return f"Éxito: {fila[0]} procesado."

    except Exception as e:
        log_web(f"❌ Error procesando {fila[0]}: {e}")
        return f"Error: {fila[0]}"
# ==========================================
# 🚀 EL NUEVO ORQUESTADOR MULTI-HILO
# ==========================================
def orquestador(query_usuario="empresas", propuesta_valor="Servicios B2B"):
    fase_recoleccion(query_usuario)
    
    log_web("\n--- ⚡ INICIANDO MODO MULTI-HILO (5 a la vez) ---")
    
    filas_brutas = sheet.get_all_values()
    tareas = []
    for i, f in enumerate(filas_brutas[1:], start=2):
        # 💡 FIX: Añadimos la propuesta_valor al paquete de tareas
        tareas.append((i, f, query_usuario, propuesta_valor))

    with ThreadPoolExecutor(max_workers=5) as executor:
        resultados = list(executor.map(procesar_prospecto_individual, tareas))

    log_web("\n🎉 ¡PROCESO MULTI-HILO COMPLETADO!")
    for res in resultados:
        print(f"  > {res}")
        
if __name__ == "__main__":
    orquestador()
