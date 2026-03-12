# ============================================================
# EXTRACTOR JIRA OPTIMIZADO - CON TIEMPO UX Y TIEMPO SW
# Google Colab | API v3 | Changelog integrado
# ============================================================
# INSTRUCCIONES DE USO:
#   1. Ejecuta la Celda 1 (instalación)
#   2. Ejecuta la Celda 2 (configuración y widgets)
#   3. Ajusta filtros y nombre de archivo
#   4. Ejecuta la Celda 3 para iniciar la extracción
# ============================================================

# ============================================================
# CELDA 1 - Instalación de dependencias
# ============================================================
"""
!pip install requests pandas openpyxl ipywidgets --quiet
"""

# ============================================================
# CELDA 2 - Configuración, clases y widgets
# ============================================================

import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
import os
import time
import shutil
import ipywidgets as widgets
from IPython.display import display
from google.colab import drive

# -- Montar Drive --------------------------------------------
drive.mount('/content/drive')

# -- CONFIGURACIÓN GLOBAL ------------------------------------
JIRA_URL     = "https://prestamype.atlassian.net"
USERNAME = st.secrets["JIRA_USER"]
API_TOKEN = st.secrets["JIRA_TOKEN"]
PROJECT_KEYS = ['PM']
DRIVE_FOLDER = '/content/drive/MyDrive/JIRA_Reports/TD'

ASSIGNEES = [
    "Angie Tomasto", "valeria vergaray",
    "crisbel aguilar", "Miguel Carreño"
]

# Estados del workflow
STATE_EN_CURSO_UX = "EN CURSO DE UX"
STATE_BACKLOG_SW  = "BACKLOG SOFTWARE | COE"
STATE_EN_CURSO_SW = "EN CURSO DE SOFTWARE | COE"
STATE_ATENDIDO    = "ATENDIDO"

# Lista completa de estados disponibles para el filtro
ALL_STATUSES = [
    "BACKLOG",
    "BACKLOG UX",
    STATE_EN_CURSO_UX,
    STATE_BACKLOG_SW,
    STATE_EN_CURSO_SW,
    STATE_ATENDIDO,
    "EN ESPERA",
    "CANCELADO",
]


# ============================================================
# WIDGETS DE CONFIGURACIÓN
# ============================================================

style_label = {'description_width': '160px'}
layout_w    = widgets.Layout(width='420px')
layout_date = widgets.Layout(width='280px')

# -- Sección: Creación de tickets ----------------------------
sec1 = widgets.HTML("<b style='color:#1a73e8'>📅 Rango de creación del ticket</b>")
date_from = widgets.DatePicker(description='Fecha inicio:', value=datetime(2025, 12, 1).date(),
                               style=style_label, layout=layout_date)
date_to   = widgets.DatePicker(description='Fecha fin:',   value=datetime(2025, 12, 31).date(),
                               style=style_label, layout=layout_date)

# -- Sección: Filtro por estado ------------------------------
sec2 = widgets.HTML("<b style='color:#1a73e8'>🔖 Estado del ticket</b>"
                    "<br><small style='color:#555'>Selecciona uno o varios estados. "
                    "Si no seleccionas ninguno, se incluyen todos.</small>")
status_filter = widgets.SelectMultiple(
    options=ALL_STATUSES,
    value=[],
    rows=8,
    layout=widgets.Layout(width='380px')
)

# -- Sección: Fecha de transición → EN CURSO UX -------------
sec3 = widgets.HTML("<b style='color:#1a73e8'>🎨 Rango de transición → EN CURSO DE UX</b>"
                    "<br><small style='color:#555'>Filtra tickets cuya primera entrada a EN CURSO UX "
                    "esté dentro de este rango. Deja vacío para no aplicar.</small>")
ux_from_toggle = widgets.Checkbox(value=False, description='Activar filtro', layout=widgets.Layout(width='200px'))
ux_date_from   = widgets.DatePicker(description='Desde:', value=datetime(2025, 12, 1).date(),
                                    style=style_label, layout=layout_date, disabled=True)
ux_date_to     = widgets.DatePicker(description='Hasta:', value=datetime(2025, 12, 31).date(),
                                    style=style_label, layout=layout_date, disabled=True)

def toggle_ux(change):
    ux_date_from.disabled = not change['new']
    ux_date_to.disabled   = not change['new']
ux_from_toggle.observe(toggle_ux, names='value')

# -- Sección: Fecha de transición → BACKLOG/EN CURSO SW -----
sec4 = widgets.HTML("<b style='color:#1a73e8'>💻 Rango de transición → BACKLOG SW / EN CURSO SW</b>"
                    "<br><small style='color:#555'>Filtra tickets cuya primera entrada a BACKLOG SW "
                    "o EN CURSO SW esté dentro de este rango. Deja vacío para no aplicar.</small>")
sw_from_toggle = widgets.Checkbox(value=False, description='Activar filtro', layout=widgets.Layout(width='200px'))
sw_date_from   = widgets.DatePicker(description='Desde:', value=datetime(2025, 12, 1).date(),
                                    style=style_label, layout=layout_date, disabled=True)
sw_date_to     = widgets.DatePicker(description='Hasta:', value=datetime(2025, 12, 31).date(),
                                    style=style_label, layout=layout_date, disabled=True)

def toggle_sw(change):
    sw_date_from.disabled = not change['new']
    sw_date_to.disabled   = not change['new']
sw_from_toggle.observe(toggle_sw, names='value')

# -- Sección: Nombre de archivo ------------------------------
sec5 = widgets.HTML("<b style='color:#1a73e8'>💾 Nombre del archivo de salida</b>")
file_name_input = widgets.Text(
    description='Nombre archivo:',
    value='reporte_jira_TD',
    placeholder='Sin extensión .xlsx',
    style=style_label, layout=layout_w
)

# -- Render de todos los widgets -----------------------------
title_label = widgets.HTML(
    "<h3 style='color:#1a73e8'>⚙️ Configuración de extracción JIRA</h3>"
)
display(
    title_label,
    sec1, date_from, date_to,
    widgets.HTML("<hr>"),
    sec2, status_filter,
    widgets.HTML("<hr>"),
    sec3, ux_from_toggle, ux_date_from, ux_date_to,
    widgets.HTML("<hr>"),
    sec4, sw_from_toggle, sw_date_from, sw_date_to,
    widgets.HTML("<hr>"),
    sec5, file_name_input,
)
print("\n[OK] Widgets cargados. Configura los filtros y ejecuta la Celda 3.")


# ============================================================
# CLASES
# ============================================================

class JiraExtractor:
    def __init__(self):
        self.auth    = HTTPBasicAuth(USERNAME, API_TOKEN)
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.base    = JIRA_URL.rstrip('/')

    def _post(self, url, payload):
        r = requests.post(url, json=payload, auth=self.auth, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def _get(self, url, params=None):
        r = requests.get(url, params=params, auth=self.auth, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def test_connection(self):
        try:
            info = self._get(f"{self.base}/rest/api/2/myself")
            print(f"[OK] Conectado como: {info.get('displayName')}")
            return True
        except Exception as e:
            print(f"[ERROR] Error de conexion: {e}")
            return False

    def fetch_issues(self, project_key, date_from_str, date_to_str):
        url = f"{self.base}/rest/api/3/search/jql"
        assignees_jql = ', '.join(f'"{a}"' for a in ASSIGNEES)
        jql = (
            f'project = "{project_key}" '
            f'AND assignee in ({assignees_jql}) '
            f'AND created >= "{date_from_str}" '
            f'AND created <= "{date_to_str}" '
            f'ORDER BY created DESC'
        )

        all_issues = []
        next_token = None
        page = 0

        while True:
            payload = {
                "jql": jql,
                "maxResults": 50,
                "fields": [
                    "key", "summary", "status", "assignee",
                    "created", "updated", "issuetype",
                    "resolution", "resolutiondate", "description", "labels",
                    "customfield_12066", "customfield_12067", "customfield_12166"
                ]
            }
            if next_token:
                payload["nextPageToken"] = next_token

            data   = self._post(url, payload)
            issues = data.get('issues', [])
            if not issues:
                break

            page += 1
            print(f"  Pagina {page}: {len(issues)} issues")
            for issue in issues:
                all_issues.append(self._parse_issue(issue, project_key))

            next_token = data.get('nextPageToken')
            if not next_token:
                break
            time.sleep(0.3)

        print(f"  * Total issues raw: {len(all_issues)}")
        return all_issues

    def _parse_issue(self, issue, project_key):
        f = issue.get('fields', {})

        def sg(obj, *keys):
            r = obj
            for k in keys:
                if isinstance(r, dict): r = r.get(k)
                else: return ''
            return r or ''

        def fmt_dt(s):
            return (s[:10] + ' ' + s[11:19]) if s and 'T' in s else (s or '')

        def fmt_d(s):
            return s[:10] if s else ''

        def clean_desc(d):
            if not d: return ''
            if isinstance(d, dict):
                parts = []
                for block in d.get('content', []):
                    for sub in block.get('content', []):
                        if sub.get('text'): parts.append(sub['text'])
                return ' '.join(parts)[:400]
            return str(d)[:400]

        def parse_arr(fd):
            if not fd: return ''
            if isinstance(fd, list):
                return ', '.join((i.get('value', str(i)) if isinstance(i, dict) else str(i)) for i in fd)
            return str(fd)

        def parse_single(fd):
            if not fd: return ''
            return fd.get('value', '') if isinstance(fd, dict) else str(fd)

        return {
            'proyecto_codigo':        project_key,
            'issue_key':              issue.get('key', ''),
            'issue_id':               issue.get('id', ''),
            'summary':                f.get('summary', ''),
            'description':            clean_desc(f.get('description')),
            'status':                 sg(f, 'status', 'name') or 'Sin estado',
            'issue_type':             sg(f, 'issuetype', 'name') or 'Sin tipo',
            'assignee':               sg(f, 'assignee', 'displayName') or 'Sin asignar',
            'created_date':           fmt_d(f.get('created', '')),
            'created_datetime':       fmt_dt(f.get('created', '')),
            'updated_date':           fmt_d(f.get('updated', '')),
            'updated_datetime':       fmt_dt(f.get('updated', '')),
            'resolution':             sg(f, 'resolution', 'name') or 'Sin resolver',
            'resolution_date':        fmt_d(f.get('resolutiondate', '')),
            'resolution_datetime':    fmt_dt(f.get('resolutiondate', '')),
            'labels':                 ', '.join(f.get('labels', [])),
            'categoria_AQN':          parse_arr(f.get('customfield_12066')),
            'PMBOK':                  parse_single(f.get('customfield_12067')),
            'informacion_completada': parse_single(f.get('customfield_12166')),
        }


class ChangelogExtractor:
    def __init__(self):
        self.auth    = HTTPBasicAuth(USERNAME, API_TOKEN)
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.base    = JIRA_URL.rstrip('/')

    def fetch_changelog(self, issue_key):
        url    = f"{self.base}/rest/api/3/issue/{issue_key}/changelog"
        result = []
        start  = 0

        while True:
            try:
                r = requests.get(url, params={'startAt': start, 'maxResults': 100},
                                 auth=self.auth, headers=self.headers)
                if r.status_code != 200:
                    break
                data   = r.json()
                values = data.get('values', [])
                if not values:
                    break

                for entry in values:
                    created = entry.get('created', '')
                    author  = entry.get('author', {}).get('displayName', 'Unknown')
                    for item in entry.get('items', []):
                        if item.get('field', '').lower() == 'status':
                            result.append({
                                'issue_key':   issue_key,
                                'from_status': item.get('fromString', ''),
                                'to_status':   item.get('toString', ''),
                                'change_dt':   created[:19].replace('T', ' ') if len(created) >= 19 else created,
                                'changed_by':  author,
                            })

                if data.get('isLast', True):
                    break
                start += 100
            except Exception:
                break

        return result

    def fetch_all(self, issue_keys):
        all_changes = []
        total = len(issue_keys)
        for i, key in enumerate(issue_keys, 1):
            if i % 20 == 0 or i == total:
                print(f"  Changelog {i}/{total} ({i/total*100:.0f}%)...")
            all_changes.extend(self.fetch_changelog(key))
            time.sleep(0.12)
        print(f"  * Total cambios: {len(all_changes)}")
        return all_changes


# ============================================================
# CÁLCULO DE TIEMPOS + FECHAS DE TRANSICIÓN
# ============================================================

def compute_times(issue_keys, changelog_list):
    """
    Retorna dict por issue_key con:
      - tiempo_ux_horas       : horas entre entrada EN CURSO UX y salida a BACKLOG/EN CURSO SW
      - tiempo_sw_horas       : horas entre entrada EN CURSO SW y ATENDIDO
      - fecha_entrada_ux      : fecha (YYYY-MM-DD) de primera entrada a EN CURSO UX
      - fecha_salida_a_sw     : fecha (YYYY-MM-DD) de primera salida hacia BACKLOG SW o EN CURSO SW
    """
    from collections import defaultdict
    cl_by_issue = defaultdict(list)
    for c in changelog_list:
        cl_by_issue[c['issue_key']].append(c)
    for key in cl_by_issue:
        cl_by_issue[key].sort(key=lambda x: x['change_dt'])

    results = {}
    for key in issue_keys:
        changes  = cl_by_issue.get(key, [])
        t_ux     = None
        t_sw     = None
        ts_ux_in = None
        ts_sw_in = None
        dt_entrada_ux  = None   # fecha primera entrada EN CURSO UX
        dt_salida_sw   = None   # fecha primera salida hacia BACKLOG/EN CURSO SW

        for c in changes:
            to_s   = c['to_status'].upper().strip()
            from_s = c['from_status'].upper().strip()
            try:
                dt = datetime.strptime(c['change_dt'], '%Y-%m-%d %H:%M:%S')
            except Exception:
                continue

            # Entrada a EN CURSO UX
            if to_s == STATE_EN_CURSO_UX and ts_ux_in is None:
                ts_ux_in      = dt
                dt_entrada_ux = dt.date()

            # Salida de EN CURSO UX → BACKLOG SW | EN CURSO SW
            if (from_s == STATE_EN_CURSO_UX and
                    to_s in (STATE_BACKLOG_SW, STATE_EN_CURSO_SW) and
                    ts_ux_in is not None and t_ux is None):
                t_ux         = round((dt - ts_ux_in).total_seconds() / 3600, 2)
                dt_salida_sw = dt.date()

            # Entrada a EN CURSO SW
            if to_s == STATE_EN_CURSO_SW and ts_sw_in is None:
                ts_sw_in = dt
                # Si no se capturó la salida desde UX, registrar la entrada directa a SW
                if dt_salida_sw is None:
                    dt_salida_sw = dt.date()

            # Salida de EN CURSO SW → ATENDIDO
            if (from_s == STATE_EN_CURSO_SW and
                    to_s == STATE_ATENDIDO and
                    ts_sw_in is not None and t_sw is None):
                t_sw = round((dt - ts_sw_in).total_seconds() / 3600, 2)

        results[key] = {
            'tiempo_ux_horas':  t_ux if t_ux is not None else '',
            'tiempo_sw_horas':  t_sw if t_sw is not None else '',
            'fecha_entrada_ux': str(dt_entrada_ux) if dt_entrada_ux else '',
            'fecha_salida_sw':  str(dt_salida_sw)  if dt_salida_sw  else '',
        }

    return results


# ============================================================
# APLICAR FILTROS POST-CHANGELOG
# ============================================================

def apply_filters(issues, time_map,
                  selected_statuses,
                  ux_filter_active, ux_from, ux_to,
                  sw_filter_active, sw_from, sw_to):
    """
    Filtra la lista de issues según:
      1. Estado del ticket (si se seleccionaron estados)
      2. Fecha de primera transición → EN CURSO UX
      3. Fecha de primera transición → BACKLOG SW / EN CURSO SW
    """
    filtered = []
    for issue in issues:
        key = issue['issue_key']
        tm  = time_map.get(key, {})

        # -- Filtro 1: Estado --------------------------------
        if selected_statuses:
            if issue['status'].upper().strip() not in [s.upper().strip() for s in selected_statuses]:
                continue

        # -- Filtro 2: Fecha entrada EN CURSO UX -------------
        if ux_filter_active:
            fecha_ux = tm.get('fecha_entrada_ux', '')
            if not fecha_ux:
                continue  # No pasó por EN CURSO UX → excluir
            try:
                fux = datetime.strptime(fecha_ux, '%Y-%m-%d').date()
                if not (ux_from <= fux <= ux_to):
                    continue
            except Exception:
                continue

        # -- Filtro 3: Fecha salida hacia BACKLOG/EN CURSO SW -
        if sw_filter_active:
            fecha_sw = tm.get('fecha_salida_sw', '')
            if not fecha_sw:
                continue  # No pasó a SW → excluir
            try:
                fsw = datetime.strptime(fecha_sw, '%Y-%m-%d').date()
                if not (sw_from <= fsw <= sw_to):
                    continue
            except Exception:
                continue

        filtered.append(issue)

    return filtered


# ============================================================
# GENERACIÓN DEL EXCEL (2 hojas: Issues + Changelog)
# ============================================================

def build_excel(issues, time_map, changelog_list, drive_folder, custom_filename):
    os.makedirs(drive_folder, exist_ok=True)

    safe_name  = "".join(c for c in custom_filename.strip() if c.isalnum() or c in (' ', '_', '-')).strip()
    safe_name  = safe_name.replace(' ', '_') or 'jira_report'
    filename   = f'{safe_name}.xlsx'
    local_path = f'/content/{filename}'
    final_path = os.path.join(drive_folder, filename)

    # -- DataFrame de issues ---------------------------------
    df = pd.DataFrame(issues)
    df['tiempo_ux_horas']  = df['issue_key'].map(lambda k: time_map.get(k, {}).get('tiempo_ux_horas', ''))
    df['tiempo_sw_horas']  = df['issue_key'].map(lambda k: time_map.get(k, {}).get('tiempo_sw_horas', ''))
    df['fecha_entrada_ux'] = df['issue_key'].map(lambda k: time_map.get(k, {}).get('fecha_entrada_ux', ''))
    df['fecha_salida_sw']  = df['issue_key'].map(lambda k: time_map.get(k, {}).get('fecha_salida_sw', ''))

    col_order = [
        'proyecto_codigo', 'issue_key', 'issue_id', 'summary',
        'status', 'issue_type', 'assignee',
        'created_date', 'created_datetime',
        'updated_date', 'updated_datetime',
        'resolution', 'resolution_date', 'resolution_datetime',
        'labels', 'categoria_AQN', 'PMBOK', 'informacion_completada',
        'tiempo_ux_horas', 'fecha_entrada_ux',
        'tiempo_sw_horas', 'fecha_salida_sw',
        'description',
    ]
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order]

    # -- DataFrame de changelog ------------------------------
    issue_keys_in_report = set(df['issue_key'].tolist())
    cl_filtered = [c for c in changelog_list if c['issue_key'] in issue_keys_in_report]
    df_cl = pd.DataFrame(cl_filtered) if cl_filtered else pd.DataFrame(
        columns=['issue_key', 'from_status', 'to_status', 'change_dt', 'changed_by']
    )
    if not df_cl.empty:
        df_cl = df_cl.sort_values(['issue_key', 'change_dt']).reset_index(drop=True)
        df_cl.columns = ['Issue Key', 'De Estado', 'A Estado', 'Fecha y Hora', 'Modificado por']

    # -- Estilos ---------------------------------------------
    from openpyxl.styles import PatternFill, Font, Alignment

    def style_header(ws, df_source, special_cols=None, special_fill='FFF2CC', special_font_color='7F6000'):
        special_cols = special_cols or set()
        for col_idx, col_name in enumerate(df_source.columns, 1):
            cell = ws.cell(row=1, column=col_idx)
            is_special = col_name in special_cols
            cell.fill      = PatternFill("solid", fgColor=special_fill if is_special else "1a73e8")
            cell.font      = Font(bold=True, color=special_font_color if is_special else "FFFFFF", size=10)
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

    col_widths_issues = {
        'proyecto_codigo': 16, 'issue_key': 14, 'issue_id': 12,
        'summary': 40, 'status': 20, 'issue_type': 16,
        'assignee': 22, 'created_date': 14, 'created_datetime': 20,
        'updated_date': 14, 'updated_datetime': 20,
        'resolution': 16, 'resolution_date': 14, 'resolution_datetime': 20,
        'labels': 20, 'categoria_AQN': 22, 'PMBOK': 18,
        'informacion_completada': 24,
        'tiempo_ux_horas': 18, 'fecha_entrada_ux': 18,
        'tiempo_sw_horas': 18, 'fecha_salida_sw': 18,
        'description': 50,
    }

    col_widths_cl = {
        'Issue Key': 14, 'De Estado': 30, 'A Estado': 30,
        'Fecha y Hora': 22, 'Modificado por': 24,
    }

    special_issue_cols = {'tiempo_ux_horas', 'fecha_entrada_ux', 'tiempo_sw_horas', 'fecha_salida_sw'}

    with pd.ExcelWriter(local_path, engine='openpyxl') as writer:

        # Hoja 1: Issues
        df.to_excel(writer, index=False, sheet_name='Issues')
        ws_i = writer.sheets['Issues']
        style_header(ws_i, df, special_cols=special_issue_cols)
        for col_idx, col_name in enumerate(df.columns, 1):
            ws_i.column_dimensions[ws_i.cell(row=1, column=col_idx).column_letter].width = col_widths_issues.get(col_name, 18)
        ws_i.freeze_panes = 'A2'

        # Hoja 2: Changelog
        df_cl.to_excel(writer, index=False, sheet_name='Changelog')
        ws_c = writer.sheets['Changelog']
        style_header(ws_c, df_cl,
                     special_cols={'De Estado', 'A Estado'},
                     special_fill='E8F0FE', special_font_color='1a3e8a')
        for col_idx, col_name in enumerate(df_cl.columns, 1):
            ws_c.column_dimensions[ws_c.cell(row=1, column=col_idx).column_letter].width = col_widths_cl.get(col_name, 20)
        ws_c.freeze_panes = 'A2'

    shutil.copy2(local_path, final_path)
    os.remove(local_path)
    print(f"\n[OK] Reporte guardado: {final_path}")
    return final_path


# ============================================================
# CELDA 3 - Ejecutar extracción completa
# ============================================================

def run_extraction():
    # Leer widgets
    date_from_str   = date_from.value.strftime('%Y-%m-%d')
    date_to_str     = date_to.value.strftime('%Y-%m-%d')
    custom_filename = file_name_input.value.strip() or 'jira_report'
    selected_statuses = list(status_filter.value)   # [] = sin filtro

    ux_filter_active = ux_from_toggle.value
    ux_from_date     = ux_date_from.value
    ux_to_date       = ux_date_to.value

    sw_filter_active = sw_from_toggle.value
    sw_from_date     = sw_date_from.value
    sw_to_date       = sw_date_to.value

    print("------------------------------------------------------------")
    print("           EXTRACTOR JIRA - REPORTE OPTIMIZADO             ")
    print("------------------------------------------------------------")
    print(f"  Creacion tickets : {date_from_str} -> {date_to_str}")
    print(f"  Estados filtro   : {selected_statuses if selected_statuses else 'Todos'}")
    print(f"  Filtro UX activo : {ux_filter_active}" + (f" ({ux_from_date} -> {ux_to_date})" if ux_filter_active else ""))
    print(f"  Filtro SW activo : {sw_filter_active}" + (f" ({sw_from_date} -> {sw_to_date})" if sw_filter_active else ""))
    print(f"  Nombre archivo   : {custom_filename}.xlsx")
    print(f"  Destino          : {DRIVE_FOLDER}")
    print("=" * 60)

    extractor = JiraExtractor()
    if not extractor.test_connection():
        return

    ch_extractor = ChangelogExtractor()
    all_issues   = []

    # Paso 1: Extraer issues por fecha de creación
    for project_key in PROJECT_KEYS:
        print(f"\nProyecto: {project_key}")
        issues = extractor.fetch_issues(project_key, date_from_str, date_to_str)
        all_issues.extend(issues)

    if not all_issues:
        print("\n[ADVERTENCIA] No se encontraron issues en el rango seleccionado.")
        return

    # Paso 2: Extraer changelog de todos los issues
    issue_keys = [i['issue_key'] for i in all_issues]
    print(f"\nExtrayendo changelog de {len(issue_keys)} issues...")
    changelog = ch_extractor.fetch_all(issue_keys)

    # Paso 3: Calcular tiempos y fechas de transición
    print("\nCalculando tiempos y fechas de transicion...")
    time_map = compute_times(issue_keys, changelog)

    # Paso 4: Aplicar filtros adicionales (estado + fechas de transición)
    print("\nAplicando filtros adicionales...")
    filtered_issues = apply_filters(
        all_issues, time_map,
        selected_statuses,
        ux_filter_active, ux_from_date, ux_to_date,
        sw_filter_active, sw_from_date, sw_to_date,
    )
    print(f"  Issues tras filtros: {len(filtered_issues)} de {len(all_issues)}")

    if not filtered_issues:
        print("\n[ADVERTENCIA] Ningún issue pasó los filtros aplicados. Ajusta los criterios.")
        return

    # Paso 5: Generar Excel con hoja Issues + hoja Changelog
    print("\nGenerando reporte Excel...")
    excel_path = build_excel(filtered_issues, time_map, changelog, DRIVE_FOLDER, custom_filename)

    ux_count = sum(1 for v in time_map.values() if v['tiempo_ux_horas'] != '')
    sw_count = sum(1 for v in time_map.values() if v['tiempo_sw_horas'] != '')

    print("\n" + "=" * 60)
    print("EXTRACCION COMPLETADA")
    print("=" * 60)
    print(f"  Issues raw extraidos  : {len(all_issues)}")
    print(f"  Issues en reporte     : {len(filtered_issues)}")
    print(f"  Tickets con Tiempo UX : {ux_count}")
    print(f"  Tickets con Tiempo SW : {sw_count}")
    print(f"  Registros changelog   : {len([c for c in changelog if c['issue_key'] in {i['issue_key'] for i in filtered_issues}])}")
    print(f"  Archivo generado      : {os.path.basename(excel_path)}")
    print("=" * 60)

# Para ejecutar, corre esta línea en una celda separada:
# run_extraction()
