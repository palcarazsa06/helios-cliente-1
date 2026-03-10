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
from bs4 import BeautifulSoup  # 💡 Mucho más ligero para la nube
from duckduckgo_search import DDGS # 💡 API interna sin bloqueos
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# 📜 SISTEMA DE LOGS
# ==========================================
def log_web(mensaje):
    with open("helios.log", "a", encoding="utf-8") as f:
        f.write(f"{mensaje}\n")
    print(mensaje)

open("helios.log", "w").close() 
log_web("🚀 HELIOS OS: Versión Cloud-Native Optimizada...")

load_dotenv()

# Configuración de LLM
llm_flash = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)
llm_creativo = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.5)

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

def conectar_google_sheets_nube():
    creds_dict = st.secrets["gcp_service_account"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

try:
    if "gcp_service_account" in st.secrets:
        cliente = conectar_google_sheets_nube()
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name('credenciales.json', scope)
        cliente = gspread.authorize(creds)
    
    # Usamos el ID de tu v1.0 que sabemos que funciona
    sheet = cliente.open_by_key("1w6ug2YFj1wpMFNwCgS2sqewUmG_4m9RIMe2x2w3Bkm4").sheet1
except Exception as e:
    log_web(f"❌ Error de conexión: {e}")
    sheet = None

# ==========================================
# 🕵️ FASE 1: RECOLECCIÓN (Usando DDGS)
# ==========================================
def fase_recoleccion(query_usuario):
    log_web(f"\n--- FASE 1: RASTREO CON DDGS (Filtro ES): {query_usuario} ---")
    nuevas = 0
    try:
        with DDGS() as ddgs:
            query_limpia = f"{query_usuario} -site:paginasamarillas.es -site:habitissimo.es"
            # 💡 FIX: Bloqueamos la región a España para evitar webs chinas o rusas
            resultados = list(ddgs.text(query_limpia, region='es-es', safesearch='moderate', max_results=10))
            
            filas_existentes = sheet.get_all_values()
            nombres_existentes = [f[0].lower().strip() for f in filas_existentes[1:] if len(f) > 0]

            for r in resultados:
                nombre = r['title'].split("-")[0].split("|")[0].strip()
                web = r['href']
                if nombre.lower() not in nombres_existentes:
                    sheet.append_row([nombre, web, "", "", "", "", "", "", "", "", "", ""])
                    nuevas += 1
        log_web(f"✅ {nuevas} empresas reales añadidas.")
    except Exception as e:
        log_web(f"❌ Error DDGS: {e}")
        
def fase_noticias(nombre_empresa):
    """
    Busca hitos, noticias o expansiones recientes de la empresa.
    """
    log_web(f"  🕵️ Investigando actualidad de {nombre_empresa}...")
    contexto_noticias = ""
    try:
        with DDGS() as ddgs:
            # Buscamos noticias, inversiones o aperturas
            query_news = f'"{nombre_empresa}" noticia expansión apertura inversión 2024 2025'
            res = list(ddgs.text(query_news, max_results=3))
            
            for r in res:
                contexto_noticias += f"- {r['title']}: {r['body']}\n"
        
        return contexto_noticias if contexto_noticias else "Sin noticias recientes destacadas."
    except Exception as e:
        log_web(f"    ⚠️ Error buscando noticias: {e}")
        return "Sin noticias recientes."

# ==========================================
# 🔬 FASE 2: CUALIFICACIÓN (BS4 + Requests)
# ==========================================
def extraer_ligero(url):
    """Extrae texto de una web sin usar navegadores pesados."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Eliminamos basura visual (scripts y estilos)
        for element in soup(["script", "style", "nav", "footer", "header"]):
            element.decompose()
            
        texto = soup.get_text(separator=' ')
        # Limpiamos espacios en blanco extra y limitamos caracteres para la ventana de contexto de Gemini
        return " ".join(texto.split())[:6000] 
    except Exception as e:
        return f"Error leyendo web: {e}"

def fase_cualificacion(fila, index, query_usuario, propuesta_valor):
    log_web(f"  🔍 Auditando {fila[0]}...")
    contexto = extraer_ligero(fila[1])
    
    # 💡 FIX: Instrucciones para que deje pasar a empresas válidas aunque no ponga "ronda" en su web
    prompt = f"""Analiza esta empresa.
    EMPRESA: {fila[0]}
    WEB: {fila[1]}
    TEXTO: {contexto}
    
    PERFIL GENERAL: {query_usuario}
    NUESTRA OFERTA: {propuesta_valor}
    
    INSTRUCCIÓN CRÍTICA: Eres un FILTRO SUAVE. Si la empresa pertenece al sector general descrito (ej. es una startup tecnológica, o es una clínica dental real), califícala como 'SI'. 
    NO exijas que la web mencione "rondas de inversión", "expansión" o noticias recientes en su portada. 
    Solo debes calificar con 'NO' si es un directorio, una página rota, una web de otro país (ej. china), o un sector totalmente distinto (ej. software industrial en vez de clínicas).

    Responde EXACTAMENTE:
    CUALIFICADO: [SI/NO]
    RESUMEN: [Máximo 30 palabras indicando su sector principal]"""
    
    try:
        res = llm_flash.invoke([HumanMessage(content=prompt)]).content
        c = "SI" if "CUALIFICADO: SI" in res.upper() else "NO"
        r = res.split("RESUMEN:")[1].strip() if "RESUMEN:" in res else "Sin datos"
        sheet.update_cell(index, 3, c)
        sheet.update_cell(index, 4, r)
    except: pass
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
# 🥷 FASE 2.5: EL NINJA DE LINKEDIN (NIVEL DIOS - RASTREADOR PURO)
# ==========================================
def investigar_linkedin_directivo(fila, index_fila):
    nombre_empresa, resumen_actual = fila[0], fila[3]
    log_web(f"  🥷 Modo Ninja: X-Ray Search para {nombre_empresa}...")
    
    time.sleep(random.uniform(2.0, 4.0)) 
    
    try:
        import requests
        import urllib.parse
        import re
        
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        query = f'"{nombre_empresa}" (CEO OR Fundador OR Director) site:linkedin.com/in/'
        url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
        
        res = requests.get(url, headers=headers, timeout=10)
        texto_bruto = re.sub(r'<[^>]+>', ' ', res.text)
        texto_bruto = re.sub(r'\s+', ' ', texto_bruto)[:8000]
        
        prompt = f"""
        Analiza este texto escaneado de internet buscando a la directiva de '{nombre_empresa}': 
        {texto_bruto}
        
        Busca un directivo real y su URL. Si NO lo encuentras, inventa un mensaje genérico.
        
        Responde EXACTAMENTE con este formato de 4 líneas (sin asteriscos):
        NOMBRE: [Su Nombre, o NADA]
        CARGO: [Su Cargo, o Responsable]
        URL: [El enlace exacto de linkedin.com/in/..., o NADA]
        MENSAJE: [Nota de invitación de LinkedIn de MÁXIMO 250 caracteres. Si hay nombre, dirígete a él. Si no, al equipo]
        """
        
        respuesta = llm_flash.invoke(prompt)
        texto = respuesta.content if hasattr(respuesta, 'content') else str(respuesta)
        
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

        if n_val:
            datos_ninja = f"{n_val} | {c_val}"
            log_web(f"    🎯 ¡Perfil localizado!: {n_val}")
            nuevo_resumen = f"{resumen_actual}\n\n[DATOS NINJA]: {datos_ninja}\nINSTRUCCIÓN EXTRA: Empieza dirigiéndote a {n_val}, sobre su perfil de {c_val}."
            sheet.update_cell(index_fila, 4, nuevo_resumen)
        else:
            log_web(f"    ⚠️ Ninja ciego, se generó mensaje genérico.")
        
        u_val = u_val.replace('<', '').replace('>', '').replace('"', '').replace("'", "")
        if u_val and "linkedin.com" in u_val and not u_val.startswith("http"):
            u_val = "https://" + u_val
            
        if not m_val:
            m_val = f"Hola, me encantaría conectar con el equipo de {nombre_empresa} para compartir sinergias. ¡Un saludo!"

        sheet.update_cell(index_fila, 10, m_val)
        sheet.update_cell(index_fila, 11, u_val)
        
    except Exception as e: 
        log_web(f"    ❌ Error Ninja: {e}")
        sheet.update_cell(index_fila, 10, f"Hola, me encantaría conectar con el equipo de {nombre_empresa} para explorar sinergias. ¡Saludos!")
        
# ==========================================
# ✍️ FASE 3: REDACCIÓN DEL CORREO (Blindada)
# ==========================================
def fase_redaccion(fila, index_fila, propuesta_valor):
    nombre, resumen, noticias = fila[0], fila[3], fila[11] # 💡 noticias es la col 12
    log_web(f"  ✍️ Redactando email hiper-personalizado para {nombre}...")
    
    prompt = f"""
    Eres un experto en Growth Engineering. Escribe un correo de puerta fría para {nombre}.
    
    CONTEXTO DE LA EMPRESA: {resumen}
    ACTUALIDAD/NOTICIAS RECIENTES: {noticias}
    NUESTRA OFERTA: {propuesta_valor}
    
    REGLAS DE ORO:
    1. Si hay una noticia real en el campo ACTUALIDAD, úsala como gancho inicial para felicitarles o mencionar que estás al tanto de su crecimiento.
    2. Si no hay noticias relevantes, usa un dato específico de su web mencionado en el CONTEXTO.
    3. NO uses saludos robóticos como "Espero que este email te encuentre bien".
    4. Sé directo, breve y enfocado en el beneficio mutuo.
    
    FORMATO:
    ASUNTO: [Asunto]
    CUERPO: [Cuerpo]
    """
    
    try:
        texto = llm_creativo.invoke([HumanMessage(content=prompt)]).content
        
        # 💡 FIX: Extracción indestructible ignorando mayúsculas, negritas y saltos de línea
        import re
        match_asunto = re.search(r"ASUNTO:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE)
        # re.DOTALL permite que el cuerpo capture todos los párrafos y saltos de línea hasta el final
        match_cuerpo = re.search(r"CUERPO:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE | re.DOTALL)
        
        if match_asunto and match_cuerpo:
            asunto = match_asunto.group(1).replace('*', '').replace('"', '').strip()
            cuerpo = match_cuerpo.group(1).replace('*', '').strip()
            
            firma = "\n\n---\nTu Nombre\nTu Cargo | Tu Empresa\n📞 +34 600 000 000 | 🌐 tuweb.com"
            cuerpo = cuerpo + firma
            
            sheet.update_cell(index_fila, 5, asunto)
            sheet.update_cell(index_fila, 6, cuerpo)
            log_web("    ✅ Textos guardados.")
        else:
            log_web("    ⚠️ La IA se saltó el formato. Aplicando Seguro de Vida.")
            # Seguro de vida por si la IA alucina: mete algo genérico pero válido
            sheet.update_cell(index_fila, 5, f"Propuesta rápida para {nombre}")
            sheet.update_cell(index_fila, 6, f"Hola equipo de {nombre},\n\nHe estado revisando vuestro proyecto y me encantaría hablar con vosotros sobre esto: {propuesta_valor}.\n\n¿Tenéis 5 minutos esta semana para comentarlo?\n\nUn saludo.")
            
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
# 🧠 PROCESAMIENTO DE UNA FILA ÚNICA (NIVEL ELITE)
# ==========================================
def procesar_prospecto_individual(datos_proceso):
    index, fila, query_usuario, propuesta_valor = datos_proceso
    while len(fila) < 12: fila.append("") # 💡 Subimos a 12 columnas para la nueva info
    
    try:
        # 1. CUALIFICACIÓN (Ya la tenemos)
        if fila[2] == "" or "ERROR" in str(fila[2]).upper():
            fase_cualificacion(fila, index, query_usuario, propuesta_valor)
            fila = sheet.row_values(index) 
        
        if str(fila[2]).upper() != "SI":
            return f"Fin: {fila[0]} (No cualificado)"

        # 2. 🕵️ NUEVO: INVESTIGACIÓN DE ACTUALIDAD (Punto 1)
        # Solo lo hacemos si la columna de noticias (columna 12) está vacía
        if not fila[11] or fila[11] == "":
            noticias = fase_noticias(fila[0])
            sheet.update_cell(index, 12, noticias)
            fila[11] = noticias # Lo guardamos en memoria para la redacción

        # 3. EMAIL & LINKEDIN (Ya los tenemos)
        if not fila[7]: buscar_email_directivo(fila, index); fila = sheet.row_values(index)
        if "[DATOS NINJA]" not in str(fila[3]): investigar_linkedin_directivo(fila, index); fila = sheet.row_values(index)

        # 4. REDACCIÓN PERSONALIZADA (Modificada abajo)
        if not fila[4]: 
            fase_redaccion(fila, index, propuesta_valor)

        return f"Éxito: {fila[0]} stalkeado y procesado."
    except Exception as e:
        return f"Error: {e}"
        
# ==========================================
# 🚀 EL ORQUESTADOR HELIOS (POTENCIA MÁXIMA)
# ==========================================
def orquestador(query_usuario="empresas", propuesta_valor="Servicios B2B"):
    """
    Orquestador principal: Recolecta nuevas empresas y procesa la base de datos.
    """
    # Paso inicial: Alimentar el sistema con prospectos reales de la web
    fase_recoleccion(query_usuario)
    
    log_web("\n--- ⚡ LANZANDO MOTOR MULTI-HILO (POTENCIA 5x) ---")
    
    # Leemos el estado actual del CRM
    filas_brutas = sheet.get_all_values()
    tareas = []
    
    # Recorremos el Excel buscando filas que necesiten trabajo
    for i, f in enumerate(filas_brutas[1:], start=2):
        # Procesamos si:
        # a) No ha sido cualificada (columna 3 vacía)
        # b) Fue cualificada como SI pero le falta el email o el borrador del mensaje
        if len(f) < 3 or f[2] == "" or (str(f[2]).upper() == "SI" and (len(f) < 8 or not f[7] or not f[4])):
            tareas.append((i, f, query_usuario, propuesta_valor))

    # Al tener tarjeta, subimos a 5 hilos para máxima velocidad
    with ThreadPoolExecutor(max_workers=5) as executor:
        resultados = list(executor.map(procesar_prospecto_individual, tareas))

    log_web("\n🎉 ¡MISIÓN COMPLETADA! Revisa tu panel en Streamlit.")
    for res in resultados:
        print(f"  > {res}")
        
if __name__ == "__main__":
    orquestador()
