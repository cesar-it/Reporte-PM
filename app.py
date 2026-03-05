import streamlit as st
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
import io
from collections import defaultdict

# -- CONFIGURACIÓN DESDE SECRETS --
JIRA_URL = "https://prestamype.atlassian.net"
USERNAME = st.secrets["JIRA_USER"]
API_TOKEN = st.secrets["JIRA_TOKEN"]
PROJECT_KEYS = ['PM']

# Estados clave para cálculo de tiempos
STATE_EN_CURSO_UX = "EN CURSO DE UX"
STATE_BACKLOG_SW  = "BACKLOG SOFTWARE | COE"
STATE_EN_CURSO_SW = "EN CURSO DE SOFTWARE | COE"
STATE_ATENDIDO    = "ATENDIDO"

# --- UTILIDADES DE FORMATEO ---
def fmt_dt(s):
    if s and 'T' in s:
        return s[:10] + ' ' + s[11:19]
    return s or ''

def fmt_d(s):
    return s[:10] if s else ''

# --- CLASES DE EXTRACCIÓN ---

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
            payload = {
                "jql": jql, 
                "maxResults": 50, 
                "fields": [
                    "key", "summary", "status", "assignee", "created", "updated", 
                    "issuetype", "resolution", "resolutiondate", "description", 
                    "labels", "customfield_12066", "customfield_12067", "customfield_12166"
                ]
            }
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
        
        # Helper para navegar el JSON de Jira
        def sg(obj, *keys):
            r = obj
            for k in keys:
                if isinstance(r, dict): r = r.get(k)
                else: return ''
            return r or ''

        # Parse de campos personalizados (Arrays o Single)
        def parse_val(fd):
            if not fd: return ''
            if isinstance(fd, list):
                return ', '.join((i.get('value', str(i)) if isinstance(i, dict) else str(i)) for i in fd)
            return fd.get('value', str(fd)) if isinstance(fd, dict) else str(fd)

        return {
            'proyecto_codigo': project_key,
            'issue_key': issue.get('key', ''),
            'issue_id': issue.get('id', ''),
            'summary': f.get('summary', ''),
            'status': sg(f, 'status', 'name'),
            'issue_type': sg(f, 'issuetype', 'name'),
            'assignee': sg(f, 'assignee', 'displayName'),
            'created_date': fmt_d(f.get('created')),
            'created_datetime': fmt_dt(f.get('created')),
            'updated_date': fmt_d(f.get('updated')),
            'updated_datetime': fmt_dt(f.get('updated')),
            'resolution': sg(f, 'resolution', 'name') or 'Sin resolver',
            'resolution_date': fmt_d(f.get('resolutiondate')),
            'resolution_datetime': fmt_dt(f.get('resolutiondate')),
            'informacion_completada': parse_val(f.get('customfield_12166')),
            'labels': ', '.join(f.get('labels', [])),
        }

class ChangelogExtractor:
    def __init__(self, username, token):
        self.auth = HTTPBasicAuth(username, token)
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.base = JIRA_URL.rstrip('/')

    def fetch_changelog(self, issue_key):
        url = f"{self.base}/rest/api/3/issue/{issue_key}/changelog"
        result = []
        try:
            r = requests.get(url, auth=self.auth, headers=self.headers)
            if r.status_code == 200:
                for entry in r.json().get('values', []):
                    created = entry.get('created', '')
                    for item in entry.get('items', []):
                        if item.get('field', '').lower() == 'status':
                            result.append({
                                'issue_key': issue_key,
                                'from_status': item.get('fromString', ''),
                                'to_status': item.get('toString', ''),
                                'change_dt': created[:19].replace('T', ' ')
                            })
        except: pass
        return result

def compute_times(issue_keys, changelog_list):
    cl_by_issue = defaultdict(list)
    for c in changelog_list:
        cl_by_issue[c['issue_key']].append(c)

    results = {}
    for key in issue_keys:
        changes = sorted(cl_by_issue.get(key, []), key=lambda x: x['change_dt'])
        t_ux, t_sw = None, None
        ts_ux_in, ts_sw_in = None, None

        for c in changes:
            to_s = c['to_status'].upper().strip()
            from_s = c['from_status'].upper().strip()
            dt = datetime.strptime(c['change_dt'], '%Y-%m-%d %H:%M:%S')

            if to_s == STATE_EN_CURSO_UX and ts_ux_in is None:
                ts_ux_in = dt
            if (from_s == STATE_EN_CURSO_UX and to_s in (STATE_BACKLOG_SW, STATE_EN_CURSO_SW) and ts_ux_in and t_ux is None):
                t_ux = round((dt - ts_ux_in).total_seconds() / 3600, 2)
            if to_s == STATE_EN_CURSO_SW and ts_sw_in is None:
                ts_sw_in = dt
            if (from_s == STATE_EN_CURSO_SW and to_s == STATE_ATENDIDO and ts_sw_in and t_sw is None):
                t_sw = round((dt - ts_sw_in).total_seconds() / 3600, 2)

        results[key] = {'tiempo_ux_horas': t_ux or '', 'tiempo_sw_horas': t_sw or ''}
    return results

# --- INTERFAZ STREAMLIT ---
st.set_page_config(page_title="Jira Reporter TD", layout="wide")
st.title("Reporte de Tiempos Jira TD 👴🏻")
# --- OCULTAR ICONO DE GITHUB Y MENÚ ---
hide_github_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stAppDeployButton {display:none;}
    [data-testid="stActionButtonIcon"] {display: none;}
    [data-testid="stAppDeployButton"] {display: none;}
    header {visibility: hidden;}
    </style>
    """
st.markdown(hide_github_style, unsafe_allow_html=True)

with st.sidebar:
    st.header("Filtros de Extracción")
    usuarios_default = ["Angie Tomasto", "Tifany Brissette Ramos Espinoza", "crisbel aguilar", "Miguel Carreño"]
    selec_usuarios = st.multiselect("Asignados:", usuarios_default, default=usuarios_default)
    fecha_inicio = st.date_input("Fecha Inicio (Creación)", value=datetime(2025, 12, 1))
    fecha_fin = st.date_input("Fecha Fin (Creación)", value=datetime.now())
    boton_ejecutar = st.button("Iniciar Extracción")

if boton_ejecutar:
    if not selec_usuarios:
        st.error("Selecciona al menos un usuario.")
    else:
        with st.spinner("1/3 Extrayendo Issues..."):
            ext = JiraExtractor(USERNAME, API_TOKEN)
            issues = ext.fetch_issues('PM', fecha_inicio.strftime('%Y-%m-%d'), fecha_fin.strftime('%Y-%m-%d'), selec_usuarios)
            
        if issues:
            with st.spinner("2/3 Procesando Historial de Estados (Changelog)..."):
                ch_ext = ChangelogExtractor(USERNAME, API_TOKEN)
                issue_keys = [i['issue_key'] for i in issues]
                all_changelogs = []
                prog_bar = st.progress(0)
                for idx, key in enumerate(issue_keys):
                    all_changelogs.extend(ch_ext.fetch_changelog(key))
                    prog_bar.progress((idx + 1) / len(issue_keys))
                
                time_map = compute_times(issue_keys, all_changelogs)
                
            with st.spinner("3/3 Consolidando Reporte..."):
                for i in issues:
                    i['tiempo_ux_horas'] = time_map.get(i['issue_key'], {}).get('tiempo_ux_horas', '')
                    i['tiempo_sw_horas'] = time_map.get(i['issue_key'], {}).get('tiempo_sw_horas', '')
                
                df = pd.DataFrame(issues)
                
                # Reordenar columnas para que coincida con tu formato anterior
                cols_order = [
                    'proyecto_codigo', 'issue_key', 'issue_id', 'summary', 'status', 
                    'issue_type', 'assignee', 'created_date', 'created_datetime', 
                    'updated_date', 'updated_datetime', 'resolution', 
                    'resolution_date', 'resolution_datetime', 'tiempo_ux_horas', 
                    'tiempo_sw_horas', 'informacion_completada'
                ]
                df = df[cols_order]

                st.success(f"Extracción exitosa: {len(df)} registros.")
                st.dataframe(df.head(20)) # Vista previa rápida
                
                # Crear Excel con formato
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Reporte Tiempos')
                    ws = writer.sheets['Reporte Tiempos']
                    
                    # Formato de cabeceras (azul para general, naranja para tiempos)
                    from openpyxl.styles import PatternFill, Font, Alignment
                    header_fill = PatternFill("solid", fgColor="1a73e8")
                    time_fill = PatternFill("solid", fgColor="FFC000")
                    white_font = Font(bold=True, color="FFFFFF")
                    
                    for cell in ws[1]:
                        if "tiempo" in cell.value:
                            cell.fill = time_fill
                        else:
                            cell.fill = header_fill
                            cell.font = white_font
                        cell.alignment = Alignment(horizontal='center')

                st.download_button(
                    label="🦀 Descargar Reporte Excel",
                    data=output.getvalue(),
                    file_name=f"reporte_jira_completo_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.warning("No se encontraron tickets con esos filtros.")



