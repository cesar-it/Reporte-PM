import streamlit as st
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
import os
import time
import io

# -- CONFIGURACIÓN JIRA (Puedes mover esto a Streamlit Secrets después) --
JIRA_URL = "https://prestamype.atlassian.net"
USERNAME = st.secrets["JIRA_USER"]
API_TOKEN = st.secrets["JIRA_TOKEN"]
PROJECT_KEYS = ['PM']

# Estados clave
STATE_EN_CURSO_UX = "EN CURSO DE UX"
STATE_BACKLOG_SW = "BACKLOG SOFTWARE | COE"
STATE_EN_CURSO_SW = "EN CURSO DE SOFTWARE | COE"
STATE_ATENDIDO = "ATENDIDO"

# --- CLASES DE EXTRACCIÓN (Tu lógica original adaptada) ---

class JiraExtractor:
    def __init__(self, username, token):
        self.auth = HTTPBasicAuth(username, token)
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.base = JIRA_URL.rstrip('/')

    def _post(self, url, payload):
        r = requests.post(url, json=payload, auth=self.auth, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def fetch_issues(self, project_key, date_from, date_to, selected_assignees):
        url = f"{self.base}/rest/api/3/search/jql"
        assignees_jql = ', '.join(f'"{a}"' for a in selected_assignees)
        jql = (
            f'project = "{project_key}" '
            f'AND assignee in ({assignees_jql}) '
            f'AND created >= "{date_from}" '
            f'AND created <= "{date_to}" '
            f'ORDER BY created DESC'
        )
        
        all_issues = []
        next_token = None
        while True:
            payload = {"jql": jql, "maxResults": 50, "fields": ["key", "summary", "status", "assignee", "created", "updated", "issuetype", "resolution", "resolutiondate", "description", "labels", "customfield_12066", "customfield_12067", "customfield_12166"]}
            if next_token: payload["nextPageToken"] = next_token
            data = self._post(url, payload)
            issues = data.get('issues', [])
            if not issues: break
            for issue in issues:
                all_issues.append(self._parse_issue(issue, project_key))
            next_token = data.get('nextPageToken')
            if not next_token: break
        return all_issues

    def _parse_issue(self, issue, project_key):
        f = issue.get('fields', {})
        sg = lambda obj, *keys: (obj.get(keys[0]).get(keys[1]) if obj.get(keys[0]) else '') if len(keys)>1 else obj.get(keys[0], '')
        
        return {
            'proyecto_codigo': project_key,
            'issue_key': issue.get('key', ''),
            'summary': f.get('summary', ''),
            'status': sg(f, 'status', 'name'),
            'assignee': sg(f, 'assignee', 'displayName'),
            'created_date': f.get('created', '')[:10],
            'labels': ', '.join(f.get('labels', [])),
            'tiempo_ux_horas': '', # Se llena luego
            'tiempo_sw_horas': '', # Se llena luego
        }

# (Aquí iría tu clase ChangelogExtractor y compute_times igual que en Colab)
# Para brevedad, asumimos que están presentes...

# --- INTERFAZ STREAMLIT ---

st.set_page_config(page_title="Jira Reporter TD", layout="wide")

st.title("📊 Extractor de Tiempos JIRA")
st.markdown("Configura los filtros y descarga el reporte en Excel.")

with st.sidebar:
    st.header("Configuración")
    usuarios_default = ["Angie Tomasto", "Tifany Brissette Ramos Espinoza", "crisbel aguilar", "Miguel Carreño"]
    selec_usuarios = st.multiselect("Usuarios a consultar:", usuarios_default, default=usuarios_default)
    
    fecha_inicio = st.date_input("Fecha Inicio", value=datetime(2025, 12, 1))
    fecha_fin = st.date_input("Fecha Fin", value=datetime.now())
    
    boton_ejecutar = st.button("🚀 Generar Reporte")

if boton_ejecutar:
    if not selec_usuarios:
        st.error("Por favor selecciona al menos un usuario.")
    else:
        with st.spinner("Extrayendo datos de Jira..."):
            extractor = JiraExtractor(USERNAME, API_TOKEN)
            # Lógica de extracción...
            datos = extractor.fetch_issues('PM', fecha_inicio.strftime('%Y-%m-%d'), fecha_fin.strftime('%Y-%m-%d'), selec_usuarios)
            
            if datos:
                df = pd.DataFrame(datos)
                st.success(f"Se encontraron {len(df)} issues.")
                st.dataframe(df) # Muestra vista previa en la web
                
                # Botón de descarga para el usuario
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                
                st.download_button(
                    label="📥 Descargar Excel",
                    data=output.getvalue(),
                    file_name=f"reporte_jira_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:

                st.warning("No se encontraron resultados.")
