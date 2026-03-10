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
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
    log_web(f"\n--- FASE 1: RASTREO HÍBRIDO (WEB + IA): {query_usuario} ---")
    nuevas = 0
    resultados_finales = []

    # 1. INTENTO A: RASTREO WEB (DuckDuckGo España)
    try:
        with DDGS() as ddgs:
            # 💡 MEJORA: Lista negra de directorios basura y agregadores en España
            lista_negra = [
                "paginasamarillas.es", "habitissimo.es", "milanuncios.com",
                "empresite.eleconomista.es", "infoisinfo.es", "expansion.com",
                "einforma.com", "infocif.es", "axesor.es", "cylex.es", "vulka.es",
                "11870.com", "zaask.es", "prontopro.es"
            ]
            
            # Construimos los operadores "-site:" dinámicamente
            filtros_exclusion = " ".join([f"-site:{dominio}" for dominio in lista_negra])
            query_limpia = f"{query_usuario} {filtros_exclusion}"
            
            # Buscamos en España con el filtro aplicado
            resultados = list(ddgs.text(query_limpia, region='es-es', safesearch='moderate', max_results=10))
            
            for r in resultados:
                resultados_finales.append({
                    "nombre": r['title'].split("-")[0].split("|")[0].strip(),
                    "web": r['href']
                })
        if len(resultados_finales) > 0:
            log_web(f"  🌐 Búsqueda web exitosa (Sin basura): {len(resultados_finales)} resultados.")
    except Exception as e:
        log_web(f"  ⚠️ Error en buscador web: {e}")

# 2. INTENTO B: PLAN DE RESCATE (Cerebro de Gemini)
    if len(resultados_finales) == 0:
        log_web("  🧱 IP bloqueada por el buscador. Activando Rescate IA...")
        
        # 💡 MEJORA 1: Prompt "Cero Tolerancia" contra alucinaciones y formatos rotos
        prompt_rescate = f"""
        Actúa como una base de datos B2B puramente transaccional de España.
        Necesito 5 empresas REALES, OPERATIVAS y ESPECÍFICAS que encajen con: '{query_usuario}'. 
        
        REGLAS DE FORMATO INQUEBRANTABLES:
        1. Devuelve EXCLUSIVAMENTE el formato Nombre||URL (una empresa por línea).
        2. CERO texto introductorio, CERO despedidas, CERO numeraciones (no pongas "1.", "2.", etc.).
        3. NO incluyes directorios, agregadores ni periódicos (nada de paginasamarillas, infoisinfo, eleconomista).
        4. Si no estás 100% seguro de la URL real de la empresa, NO la incluyas. No inventes dominios.
        
        EJEMPLO DE SALIDA EXACTA:
        Iberdrola||https://www.iberdrola.es
        Repsol||https://www.repsol.es
        """
        try:
            res_ia = llm_flash.invoke([HumanMessage(content=prompt_rescate)]).content
            
            for linea in res_ia.split('\n'):
                linea = linea.strip()
                if "||" in linea:
                    partes = linea.split("||")
                    nombre = partes[0].replace("*", "").strip()
                    web = partes[1].replace("*", "").strip()
                    
                    # 💡 MEJORA 2: Limpieza de numeraciones. Si la IA pone "1. Empresa", esto deja solo "Empresa"
                    import re
                    nombre = re.sub(r'^\d+[\.\-\)]\s*', '', nombre)
                    
                    # 💡 MEJORA 3: Aseguramos el protocolo y bloqueamos basura a nivel de código
                    web = web.lower()
                    if not web.startswith("http"):
                        web = "https://" + web
                        
                    directorios_basura = ["paginasamarillas", "habitissimo", "milanuncios", "eleconomista", "infoisinfo", "expansion"]
                    
                    # Solo la añadimos si tiene nombre, web y NO es un directorio
                    if nombre and web and not any(basura in web for basura in directorios_basura):
                        resultados_finales.append({
                            "nombre": nombre, 
                            "web": web
                        })
        except Exception as e:
            log_web(f"  ❌ Error fatal en Rescate IA: {e}")

    # 3. FILTRADO E INSERCIÓN EN CRM
    try:
        filas_existentes = sheet.get_all_values()
        nombres_existentes = [f[0].lower().strip() for f in filas_existentes[1:] if len(f) > 0]

        for emp in resultados_finales:
            nombre_limpio = emp['nombre']
            web_limpia = emp['web']
            
            # Limpieza básica para evitar basura
            if nombre_limpio.lower() not in nombres_existentes and "http" in web_limpia.lower():
                # Insertamos 12 columnas vacías para que el Modo Noticias tenga su sitio
                sheet.append_row([nombre_limpio, web_limpia, "", "", "", "", "", "", "", "", "", ""])
                nuevas += 1
                
        log_web(f"✅ Se han añadido {nuevas} empresas al CRM.")
    except Exception as e:
        log_web(f"❌ Error insertando en Google Sheets: {e}")
        

# ==========================================
# 🕵️ FASE 1.5: EL STALKER (Noticias e Hitos)
# ==========================================
def fase_noticias(nombre_empresa):
    """
    Busca en internet qué ha estado haciendo la empresa en el ÚLTIMO AÑO 
    para usarlo como "rompehielos" en el email.
    """
    log_web(f"  🕵️ Stalkeando actualidad de {nombre_empresa}...")
    texto_noticias = ""
    
    # 1. BATERÍA DE PALABRAS CLAVE (El Arsenal del Stalker)
    # Cubrimos 3 frentes: Financiero (inversión/ronda), Operativo (apertura/acuerdo) y PR (premio/lanzamiento)
    # Excluimos directorios como infojobs o axesor para que la "noticia" no sea una oferta de empleo o un registro mercantil.
    query = f'"{nombre_empresa}" (noticia OR expansión OR inversión OR ronda OR acuerdo OR lanzamiento OR premio OR apertura OR adquisición OR facturación) -site:infojobs.net -site:axesor.es -site:infocif.es'
    
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            # 💡 SUGERENCIA PRO APLICADA: timelimit='y'
            # Esto obliga a DuckDuckGo a buscar SOLO contenido indexado en los últimos 12 meses.
            # Así garantizamos que el hielo que rompemos es información fresca.
            resultados = list(ddgs.text(query, region='es-es', timelimit='y', max_results=3))
            
            # 3. Extraemos el jugo de cada resultado
            for r in resultados:
                titulo = r.get('title', '')
                cuerpo = r.get('body', '')
                texto_noticias += f"- {titulo}: {cuerpo}\n"
                
        # 4. Verificación final
        if texto_noticias.strip() != "":
            return texto_noticias
        else:
            return "Sin noticias recientes en el último año."
            
    except Exception as e:
        log_web(f"    ⚠️ Stalker bloqueado para {nombre_empresa}: {e}")
        return "No se pudieron obtener noticias por bloqueo de red."

# ==========================================
# 🔬 FASE 2: CUALIFICACIÓN (BS4 + Requests)
# ==========================================
def extraer_ligero(url):
    """
    Extrae texto de una web saltándose bloqueos básicos, 
    errores de certificado SSL y limpiando el ruido visual.
    """
    try:
        # 1. DISFRAZ PERFECTO: Añadimos cabeceras para parecer un navegador Chrome de España
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        }
        
        # 2. ESCUDO ANTI-CAÍDAS: verify=False ignora si la web tiene el certificado HTTPS caducado
        res = requests.get(url, headers=headers, timeout=12, verify=False)
        res.raise_for_status()
        
        # 3. PROTECCIÓN DE IDIOMA: Forzamos UTF-8 para que las 'Ñ' y tildes se lean bien
        if res.encoding is None or res.encoding == 'ISO-8859-1':
            res.encoding = 'utf-8'
            
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 4. LIMPIEZA QUIRÚRGICA: Añadimos "aside" y "noscript" a tu lista para quitar banners de cookies
        for element in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
            element.decompose()
            
        texto = soup.get_text(separator=' ')
        texto_limpio = " ".join(texto.split())
        
        # 5. SEGURO DE VIDA: Si la web es un marco vacío, avisamos a la IA
        if len(texto_limpio) < 50:
            return "Error: La web parece estar vacía o bloquea la extracción de texto."
            
        return texto_limpio[:6000] 
        
    except requests.exceptions.HTTPError as e:
        # Si choca contra Cloudflare (Error 403), le damos un aviso útil a la IA en vez de un simple error
        if e.response.status_code == 403 or e.response.status_code == 406:
            return "Error 403: La web tiene un escudo anti-bots severo. Intenta cualificar la empresa basándote solo en su nombre y en tu conocimiento del sector."
        return f"Error HTTP: {e}"
    except requests.exceptions.SSLError:
        return "Error SSL: El servidor bloqueó la conexión segura."
    except Exception as e:
        return f"Error leyendo web: {e}"

def fase_cualificacion(fila, index, query_usuario, propuesta_valor):
    log_web(f"  🔍 Auditando {fila[0]}...")
    contexto = extraer_ligero(fila[1])
    
    # 💡 MEJORA 1: Prompt a prueba de balas que sabe manejar bloqueos (Error 403)
    prompt = f"""Actúa como auditor B2B experto.
    EMPRESA: {fila[0]}
    WEB: {fila[1]}
    TEXTO EXTRAÍDO: {contexto}
    
    PERFIL BUSCADO: '{query_usuario}'
    NUESTRA OFERTA: '{propuesta_valor}'
    
    INSTRUCCIONES CRÍTICAS:
    1. Eres un FILTRO SUAVE. Si la empresa parece pertenecer al sector general buscado, califícala como 'SI'.
    2. 🚨 ESCUDO ANTI-BOTS: Si el TEXTO EXTRAÍDO indica "Error 403", "Error SSL" o está vacío, NO la descartes automáticamente. Deduce si es válida guiándote SOLO por el nombre de la empresa y la URL.
    3. Descarta con 'NO' SOLAMENTE si es un directorio (Páginas Amarillas), de otro país/idioma que no encaja, o un sector totalmente opuesto.

    Responde EXACTAMENTE con este formato de dos líneas (Sin asteriscos ni negritas):
    CUALIFICADO: [SI o NO]
    RESUMEN: [Máximo 30 palabras indicando a qué se dedican y por qué los pasas o descartas]
    """
    
    try:
        res = llm_flash.invoke([HumanMessage(content=prompt)]).content
        
        # 💡 MEJORA 2: Extracción Indestructible con Regex
        match_c = re.search(r"CUALIFICADO:\s*(SI|NO)", res, re.IGNORECASE)
        match_r = re.search(r"RESUMEN:\s*(.*)", res, re.IGNORECASE | re.DOTALL)
        
        c = match_c.group(1).upper() if match_c else "NO"
        r = match_r.group(1).replace('*', '').strip() if match_r else "Fallo en lectura de IA."
        
        sheet.update_cell(index, 3, c)
        sheet.update_cell(index, 4, r)
        
    except Exception as e:
        # 💡 MEJORA 3: Adiós al "except: pass". Si falla, lo marcamos para no entrar en bucle.
        log_web(f"    ❌ Error en auditoría de {fila[0]}: {e}")
        sheet.update_cell(index, 3, "ERROR")
        sheet.update_cell(index, 4, "Fallo en API de cualificación.")
        
# ==========================================
# 🎯 FASE 2.5: BUSCADOR DE CORREOS (Hunter API)
# ==========================================
def buscar_email_directivo(fila, index_fila):
    nombre, web = fila[0], fila[1]
    log_web(f"  🎯 Buscando email en Hunter.io para {nombre}...")
    
    # 1. Extracción y limpieza del dominio
    try: 
        dominio = urlparse(web).netloc.replace('www.', '')
    except: 
        dominio = ""
        
    if not dominio:
        sheet.update_cell(index_fila, 8, "Dominio inválido")
        return
        
    # 2. Gestión robusta de la API Key (Streamlit Secrets vs Local)
    import streamlit as st
    api_key = st.secrets.get("HUNTER_API_KEY", os.getenv('HUNTER_API_KEY'))
    
    if not api_key:
        log_web("    ⚠️ Aviso: No hay HUNTER_API_KEY configurada.")
        sheet.update_cell(index_fila, 8, f"info@{dominio}")
        return

    url = f"https://api.hunter.io/v2/domain-search?domain={dominio}&api_key={api_key}"
    
    try:
        # 3. Petición con Timeout de seguridad (10 segundos)
        res = requests.get(url, timeout=10)
        
        # 4. Control de saldo y errores de API
        if res.status_code != 200:
            log_web(f"    ⚠️ Error de Hunter API ({res.status_code}). Puede que no tengas saldo.")
            sheet.update_cell(index_fila, 8, f"info@{dominio}")
            return
            
        datos = res.json()
        emails = datos.get('data', {}).get('emails', [])
        
        if not emails:
            log_web(f"    ⚠️ Hunter no encontró emails para este dominio.")
            sheet.update_cell(index_fila, 8, f"info@{dominio}")
            return

        # 5. MEJORA CLAVE: Filtro Inteligente de Prioridad (Personal vs Genérico)
        email_encontrado = None
        
        # Pasada 1: Buscamos específicamente un correo que Hunter marque como 'personal'
        for email_data in emails:
            if email_data.get('type') == 'personal':
                email_encontrado = email_data.get('value')
                break # ¡Premio! Encontramos a un humano. Rompemos el bucle.
        
        # Pasada 2: Si no hay personales, nos conformamos con el mejor valorado (el primero)
        if not email_encontrado:
            email_encontrado = emails[0].get('value')

        log_web(f"    🎯 ¡Blanco fijado!: {email_encontrado}")
        sheet.update_cell(index_fila, 8, email_encontrado)
        
    except requests.exceptions.Timeout:
        log_web("    ❌ Timeout: Hunter.io tardó demasiado en responder.")
        sheet.update_cell(index_fila, 8, f"info@{dominio}")
    except Exception as e:
        log_web(f"    ❌ Error general en Hunter: {e}")
        sheet.update_cell(index_fila, 8, f"info@{dominio}")

# ==========================================
# 🥷 FASE 2.5: EL NINJA DE LINKEDIN (NIVEL SOMBRA)
# ==========================================
def investigar_linkedin_directivo(fila, index_fila):
    nombre_empresa, resumen_actual = fila[0], fila[3]
    log_web(f"  🥷 Modo Ninja: X-Ray Search en LinkedIn para {nombre_empresa}...")
    
    try:
        from duckduckgo_search import DDGS
        import re
        
        # 1. BÚSQUEDA RAYOS X (Estructurada y Segura)
        query = f'"{nombre_empresa}" (CEO OR Fundador OR Director OR "Managing Director") site:linkedin.com/in/'
        contexto_busqueda = ""
        
        with DDGS() as ddgs:
            # region='es-es' prioriza perfiles de España. max_results=3 es suficiente para no marear a la IA.
            resultados = list(ddgs.text(query, region='es-es', safesearch='moderate', max_results=3))
            
            # Preparamos un contexto súper limpio para la IA
            for r in resultados:
                contexto_busqueda += f"TÍTULO: {r.get('title')}\nURL: {r.get('href')}\nINFO: {r.get('body')}\n---\n"
                
        if not contexto_busqueda:
            log_web("    ⚠️ El radar Ninja fue bloqueado o no encontró perfiles.")
            contexto_busqueda = "Sin resultados en la búsqueda."

        # 2. EXTRACCIÓN CON LLM (Prompt Quirúrgico)
        prompt = f"""
        Analiza estos resultados de búsqueda de LinkedIn para la empresa '{nombre_empresa}': 
        {contexto_busqueda}
        
        Busca al directivo principal (CEO, Fundador, Director). Si en el texto NO ves un perfil claro, devuelve NADA.
        
        Responde EXACTAMENTE con este formato de 4 líneas (sin asteriscos):
        NOMBRE: [Su Nombre, o NADA]
        CARGO: [Su Cargo, o NADA]
        URL: [El enlace exacto de linkedin.com/in/..., o NADA]
        MENSAJE: [Nota de invitación de LinkedIn de MÁXIMO 250 caracteres dirigida a él. Si no hay nombre, mensaje genérico al equipo]
        """
        
        respuesta = llm_flash.invoke([HumanMessage(content=prompt)]).content
        
        # 3. EXTRACCIÓN INDESTRUCTIBLE (Regex)
        n_ninja = re.search(r"NOMBRE:\s*\*?\*?\s*(.*)", respuesta, re.IGNORECASE)
        c_ninja = re.search(r"CARGO:\s*\*?\*?\s*(.*)", respuesta, re.IGNORECASE)
        u_ninja = re.search(r"URL:\s*\*?\*?\s*(.*)", respuesta, re.IGNORECASE)
        m_ninja = re.search(r"MENSAJE:\s*\*?\*?\s*(.*)", respuesta, re.IGNORECASE | re.DOTALL)

        n_val = n_ninja.group(1).replace('*', '').strip() if n_ninja else ""
        c_val = c_ninja.group(1).replace('*', '').strip() if c_ninja else ""
        u_val = u_ninja.group(1).replace('*', '').strip() if u_ninja else ""
        m_val = m_ninja.group(1).replace('*', '').strip() if m_ninja else ""

        # Control de alucinaciones
        if n_val.upper() in ["NADA", "NO ENCONTRADO", "NONE"]: n_val = ""
        if u_val.upper() in ["NADA", "NO ENCONTRADO", "NONE"]: u_val = ""

        # 4. ACTUALIZACIÓN DE DATOS
        if n_val:
            datos_ninja = f"{n_val} | {c_val}"
            log_web(f"    🎯 ¡Perfil localizado!: {n_val}")
            nuevo_resumen = f"{resumen_actual}\n\n[DATOS NINJA]: {datos_ninja}\nINSTRUCCIÓN EXTRA: Empieza dirigiéndote a {n_val}, sobre su perfil de {c_val}."
            sheet.update_cell(index_fila, 4, nuevo_resumen)
        else:
            log_web(f"    ⚠️ Ninja ciego, se generó mensaje genérico.")
        
        # Limpieza final de URL
        u_val = u_val.replace('<', '').replace('>', '').replace('"', '').replace("'", "")
        if u_val and "linkedin.com" in u_val and not u_val.startswith("http"):
            u_val = "https://" + u_val
            
        if not m_val or m_val.upper() in ["NADA", "NONE"]:
            m_val = f"Hola, me encantaría conectar con el equipo de {nombre_empresa} para compartir sinergias. ¡Un saludo!"

        sheet.update_cell(index_fila, 10, m_val)
        sheet.update_cell(index_fila, 11, u_val)
        
    except Exception as e: 
        log_web(f"    ❌ Error Ninja: {e}")
        sheet.update_cell(index_fila, 10, f"Hola, me encantaría explorar sinergias con el equipo de {nombre_empresa}. ¡Saludos!")


# ==========================================
# ✍️ FASE 3: REDACCIÓN DEL CORREO (Copywriting Élite)
# ==========================================
def fase_redaccion(fila, index_fila, propuesta_valor):
    nombre_empresa = fila[0]
    resumen_ninja = fila[3] # 💡 Contiene el resumen web + los Datos Ninja (Nombre directivo)
    noticias = fila[11]     # 💡 noticias es la col 12
    
    log_web(f"  ✍️ Redactando email hiper-personalizado para {nombre_empresa}...")
    
    # 💡 MEJORA 1: Prompt con Psicología B2B y Reglas Anti-Spam
    prompt = f"""
    Eres un experto en Growth Engineering y Copywriting B2B de respuesta directa. Escribe un 'cold email' para la empresa '{nombre_empresa}'.
    
    CONTEXTO DE LA EMPRESA Y DIRECTIVO: {resumen_ninja}
    ACTUALIDAD/NOTICIAS RECIENTES: {noticias}
    NUESTRA OFERTA: {propuesta_valor}
    
    REGLAS DE ORO (Si rompes una, el email fracasará):
    1. DESTINATARIO: Analiza el CONTEXTO. Si detectas un nombre de directivo (ej: Datos Ninja), DEBES empezar el correo dirigiéndote a esa persona por su nombre de pila (ej: "Hola [Nombre],"). Si no hay nombre, usa un saludo muy informal al equipo.
    2. ROMPEHIELOS: Usa la ACTUALIDAD como gancho en la primera frase para demostrar que les sigues la pista. Si no hay noticias, menciona un dato específico de su web sacado del CONTEXTO.
    3. LONGITUD Y TONO: Sé ultra-conciso (máximo 60-70 palabras). Escribe como un humano ocupado escribiendo a otro humano ocupado desde su móvil. CERO emojis. CERO saludos formales tipo "Espero que este correo te encuentre bien".
    4. CTA (Llamada a la acción): No pidas una reunión de 30 minutos. Termina con una pregunta de muy baja fricción (ej: "¿Te hace sentido?", "¿Os interesaría echarle un ojo a cómo funciona?").
    5. ASUNTO: Debe parecer un correo interno o de un cliente. Máximo 2 a 4 palabras, SIN emojis, e idealmente TODO EN MINÚSCULAS (ej: "vuestra nueva sede" o "duda sobre {nombre_empresa}").
    
    FORMATO EXACTO DE RESPUESTA:
    ASUNTO: [Asunto corto en minúsculas]
    CUERPO: [Cuerpo del correo]
    """
    
    try:
        texto = llm_creativo.invoke([HumanMessage(content=prompt)]).content
        
        import re
        match_asunto = re.search(r"ASUNTO:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE)
        match_cuerpo = re.search(r"CUERPO:\s*\*?\*?\s*(.*)", texto, re.IGNORECASE | re.DOTALL)
        
        if match_asunto and match_cuerpo:
            # Limpieza extrema de asteriscos y comillas que la IA a veces pone en el asunto
            asunto = match_asunto.group(1).replace('*', '').replace('"', '').replace("'", "").strip()
            cuerpo = match_cuerpo.group(1).replace('*', '').strip()
            
            # 💡 MEJORA 2: Firma minimalista (Convierte más que firmas súper recargadas)
            firma = "\n\n---\nTu Nombre\nGrowth Engineer | Tu Empresa\n📞 +34 600 000 000"
            cuerpo = cuerpo + firma
            
            sheet.update_cell(index_fila, 5, asunto)
            sheet.update_cell(index_fila, 6, cuerpo)
            log_web("    ✅ Textos persuasivos generados y guardados.")
        else:
            log_web("    ⚠️ La IA se saltó el formato. Aplicando Seguro de Vida.")
            sheet.update_cell(index_fila, 5, f"duda rápida sobre {nombre_empresa}")
            sheet.update_cell(index_fila, 6, f"Hola equipo,\n\nEstaba revisando la web de {nombre_empresa} y me pareció interesante lo que hacéis.\n\nHe pensado que esto os podría encajar: {propuesta_valor}.\n\n¿Os interesaría que os pase más info?\n\nUn saludo.")
            
    except Exception as e:
        log_web(f"    ❌ Error en redacción: {e}")

# ==========================================
# 📩 FASE 4: EL CARTERO (Envío Seguro)
# ==========================================
def fase_envio(fila, index_fila, servidor):
    nombre, asunto, cuerpo = fila[0], fila[4], fila[5]
    email_destino = str(fila[7]).strip()
    asunto_limpio = str(asunto).strip().replace('\n', '').replace('\r', '')
    
    log_web(f"  📩 Preparando envío para {nombre} a la dirección: {email_destino}...")

    # 💡 MEJORA 1: Validar que realmente hay un email válido antes de molestar al servidor SMTP
    if "@" not in email_destino or "No encontrado" in email_destino:
        log_web(f"    ❌ Envío cancelado: La dirección '{email_destino}' no es válida.")
        return False

    try:
        # Construcción del correo
        msg = EmailMessage()
        msg.set_content(cuerpo)
        msg['Subject'] = asunto_limpio
        
        # 💡 MEJORA 2: Priorizamos sacar el usuario de los Secrets de Streamlit por seguridad
        import streamlit as st
        remitente = st.secrets.get("GMAIL_USER", os.getenv("GMAIL_USER"))
        msg['From'] = remitente
        msg['To'] = email_destino 
        
        # Envío real
        servidor.send_message(msg)
        
        # 💡 MEJORA 3: Solo marcamos como "SI" si el servidor no dio error
        sheet.update_cell(index_fila, 7, "SI")
        log_web(f"    🚀 ¡BUM! Correo enviado con éxito a {email_destino}.")
        return True
        
    except Exception as e:
        log_web(f"    ❌ Error crítico al enviar por SMTP: {e}")
        return False

# ==========================================
# 🚀 FASE 4.5: EL BOTÓN ROJO (Envío Manual)
# ==========================================
def enviar_correo_manual(nombre_empresa, nuevo_asunto=None, nuevo_cuerpo=None):
    log_web(f"\n🚀 Lanzando envío manual aprobado para: {nombre_empresa}")
    try:
        import streamlit as st
        filas = sheet.get_all_values()
        
        for index, fila in enumerate(filas[1:], start=2):
            if fila[0] == nombre_empresa:
                # Comprobación de seguridad para no enviar dos veces
                if str(fila[6]).strip().upper() != "SI":
                    
                    # 1. Actualizamos el Excel con tus ediciones manuales
                    if nuevo_asunto: 
                        sheet.update_cell(index, 5, nuevo_asunto)
                        fila[4] = nuevo_asunto
                    if nuevo_cuerpo: 
                        sheet.update_cell(index, 6, nuevo_cuerpo)
                        fila[5] = nuevo_cuerpo
                        
                    # 2. Extraemos credenciales (Prioridad: Secrets de la nube)
                    usuario = st.secrets.get("GMAIL_USER", os.getenv("GMAIL_USER"))
                    password = st.secrets.get("GMAIL_PASS", os.getenv("GMAIL_PASS"))
                    
                    if not usuario or not password:
                        log_web("    ❌ Error: Falta GMAIL_USER o GMAIL_PASS en los Secrets.")
                        return False

                    # 3. Conexión SMTP Auto-Gestionada (Evita conexiones Zombi)
                    try:
                        with smtplib.SMTP('smtp.gmail.com', 587) as servidor_smtp:
                            servidor_smtp.ehlo() # Saludo oficial al servidor
                            servidor_smtp.starttls()
                            servidor_smtp.login(usuario, password)
                            
                            # Llamamos al Cartero (que mejoramos en el paso anterior)
                            fase_envio(fila, index, servidor_smtp)
                            return True
                            
                    except smtplib.SMTPAuthenticationError:
                        log_web("    ❌ Error de Autenticación: Google ha rechazado el inicio de sesión. Revisa tu Contraseña de Aplicación de 16 dígitos.")
                        return False
                    except Exception as e:
                        log_web(f"    ❌ Error de red con Gmail: {e}")
                        return False
                else:
                    log_web(f"    ⚠️ Operación cancelada: El correo para {nombre_empresa} ya fue enviado previamente.")
                    return False
                    
        log_web(f"    ❌ No se encontró la empresa {nombre_empresa} en la base de datos.")
        return False
        
    except Exception as e: 
        log_web(f"    ❌ Error general en la preparación del envío manual: {e}")
        return False
        
import time
import random

# ==========================================
# 🧠 PROCESAMIENTO DE UNA FILA ÚNICA (NIVEL ÉLITE)
# ==========================================
def procesar_prospecto_individual(datos_proceso):
    index, fila, query_usuario, propuesta_valor = datos_proceso
    
    # 1. Aseguramos la estructura en memoria antes de empezar
    while len(fila) < 12: fila.append("") 
    
    # 💡 MEJORA 1: Función interna con "Escudo Anti-Baneo" para refrescar datos
    # Si Google Sheets nos bloquea por un microsegundo, esto espera y reintenta en lugar de crashear todo el programa.
    def refrescar_fila_seguro():
        for intento in range(3):
            try:
                time.sleep(random.uniform(0.5, 1.5)) # Micro-pausa inteligente
                nueva_fila = sheet.row_values(index)
                while len(nueva_fila) < 12: nueva_fila.append("")
                return nueva_fila
            except Exception as e:
                if intento == 2: raise e
                log_web(f"    ⏳ Google Sheets saturado. Reintentando lectura de {fila[0]}...")
                time.sleep(2)

    try:
        # ---------------------------------------------------------
        # FASE 1: CUALIFICACIÓN
        # ---------------------------------------------------------
        if str(fila[2]).strip() == "" or "ERROR" in str(fila[2]).upper():
            fase_cualificacion(fila, index, query_usuario, propuesta_valor)
            fila = refrescar_fila_seguro() 
        
        # Cortafuegos: Si no cualifica, abortamos misión y no gastamos créditos de IA ni de Hunter
        if str(fila[2]).strip().upper() != "SI":
            return f"🛑 Descartado: {fila[0]}"

        # ---------------------------------------------------------
        # FASE 2: EL STALKER (Noticias en columna L)
        # ---------------------------------------------------------
        if not fila[11] or str(fila[11]).strip() == "":
            noticias = fase_noticias(fila[0])
            sheet.update_cell(index, 12, noticias)
            # 💡 MEJORA 2: Actualizamos la memoria local. ¡Ahorramos 1 llamada de lectura a Google!
            fila[11] = noticias 

        # ---------------------------------------------------------
        # FASE 3: FRANCOTIRADOR (Hunter)
        # ---------------------------------------------------------
        if not fila[7] or str(fila[7]).strip() == "":
            buscar_email_directivo(fila, index)
            # 💡 MEJORA 3: NO refrescamos la fila aquí. 
            # El copywriter no necesita saber el email para redactar el asunto, así que ahorramos otra llamada a Google.

        # ---------------------------------------------------------
        # FASE 4: EL NINJA (LinkedIn)
        # ---------------------------------------------------------
        if "[DATOS NINJA]" not in str(fila[3]):
            investigar_linkedin_directivo(fila, index)
            # 💡 Aquí SÍ necesitamos refrescar, porque el Ninja ha inyectado el nombre del CEO en la celda 4 
            # y el copywriter lo necesita leer para decir "Hola [Nombre]".
            fila = refrescar_fila_seguro()

        # ---------------------------------------------------------
        # FASE 5: COPYWRITER (Redacción)
        # ---------------------------------------------------------
        if not fila[4] or str(fila[4]).strip() == "":
            fase_redaccion(fila, index, propuesta_valor)

        return f"✅ {fila[0]} procesado de principio a fin."
        
    except Exception as e:
        log_web(f"  ❌ Error fatal en el orquestador para {fila[0]}: {e}")
        return f"❌ Error general en {fila[0]}: {e}"
        
# ==========================================
# 🚀 EL ORQUESTADOR HELIOS (POTENCIA MÁXIMA)
# ==========================================
def orquestador(query_usuario="empresas", propuesta_valor="Servicios B2B"):
    """
    Orquestador principal: Recolecta nuevas empresas y procesa la base de datos de forma segura.
    """
    try:
        # 1. Alimentar el sistema con sangre fresca
        fase_recoleccion(query_usuario)
        
        log_web("\n--- ⚡ LANZANDO MOTOR MULTI-HILO (CEREBRO HELIOS) ---")
        
        # 2. Lectura del estado actual de la base de datos
        try:
            filas_brutas = sheet.get_all_values()
        except Exception as e:
            log_web(f"  ❌ Error crítico conectando con Google Sheets: {e}")
            return
            
        tareas = []
        
        # 3. FILTRO DE TAREAS (La Matriz de 12 Columnas)
        for i, f in enumerate(filas_brutas[1:], start=2):
            # Normalizamos la fila a 12 columnas para evitar errores de "Index Out of Range"
            while len(f) < 12: f.append("")
            
            # Condición A: No ha sido auditada o dio un error temporal
            necesita_auditoria = str(f[2]).strip() == "" or "ERROR" in str(f[2]).upper()
            
            # Condición B: Es un lead válido ('SI') pero le falta algún dato clave para enviar el email
            es_valido = str(f[2]).strip().upper() == "SI"
            # f[4] = Asunto, f[7] = Email, f[9] = Mensaje LinkedIn, f[11] = Noticias
            le_faltan_datos = es_valido and (not f[4] or not f[7] or not f[9] or not f[11])
            
            if necesita_auditoria or le_faltan_datos:
                tareas.append((i, f, query_usuario, propuesta_valor))

        # 4. PROTECCIÓN ANTI-BANEOS (Sistema de Lotes)
        # Procesar de 15 en 15 asegura que nunca excedemos los límites de la API en una sola tirada
        MAX_LOTE = 15
        tareas_a_procesar = tareas[:MAX_LOTE]
        
        if not tareas_a_procesar:
            log_web("  ℹ️ No hay prospectos pendientes en esta ronda. CRM al día.")
            return
            
        if len(tareas) > MAX_LOTE:
            log_web(f"  ⚠️ Hay {len(tareas)} leads en cola. Procesando lote seguro de {MAX_LOTE} para proteger la cuota de la API...")

        # 5. EJECUCIÓN MULTI-HILO DESFASADA
        from concurrent.futures import ThreadPoolExecutor
        import time
        import random
        
        # 💡 MEJORA CLAVE: En la nube, 4 hilos es el "Sweet Spot" de estabilidad para Sheets.
        HILOS = 4 
        log_web(f"  🚀 Inyectando {len(tareas_a_procesar)} tareas en {HILOS} hilos de procesamiento...")
        
        # Función envoltorio para que los hilos no ataquen la API exactamente a la vez
        def ejecutar_con_retraso(tarea):
            time.sleep(random.uniform(0.1, 1.2)) # Cada hilo arranca con 1 segundo de diferencia
            return procesar_prospecto_individual(tarea)

        with ThreadPoolExecutor(max_workers=HILOS) as executor:
            resultados = list(executor.map(ejecutar_con_retraso, tareas_a_procesar))

        # 6. REPORTE FINAL
        log_web("\n🎉 ¡MISIÓN COMPLETADA! Revisa tu panel en Streamlit.")
        for res in resultados:
            print(f"  > {res}")
            # Opcional: Si quieres que el resumen también salga en la pantalla de Streamlit
            # log_web(f"  > {res}") 

    except Exception as e:
        log_web(f"\n❌ Error catastrófico en el Orquestador: {e}")
        
if __name__ == "__main__":
    orquestador()
