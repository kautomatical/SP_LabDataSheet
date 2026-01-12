import streamlit as st
import json
import gspread
import pandas as pd
from exporter import export_to_word
import datetime
import os
import io
import zipfile
from google.auth.transport import requests # Moved to top-level import
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow # Moved to top-level import
from streamlit_cookies_manager import EncryptedCookieManager # New Import

@st.cache_data
def load_config(file_path):
    """Loads a JSON configuration file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_config(file_path, config_data):
    """Saves a configuration dictionary to a JSON file."""
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=4, ensure_ascii=False)

# Load language strings once
LANG_CONFIG = load_config('languages.json')
LANG = "zh-tw"
l = LANG_CONFIG[LANG]

# --- Page Configuration & Constants ---
st.set_page_config(
    page_title=l.get("page_title", "Lab Data Sheet"),
    page_icon=l.get("page_icon", "🔬"),
    layout="centered"
)
SCOPES = ["openid", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]

# --- OAuth 2.0 Functions ---

def get_google_flow():
    """Initializes and returns the Google OAuth Flow object."""
    client_secrets = {
        "web": {
            "client_id": st.secrets["GOOGLE_CLIENT_ID"],
            "client_secret": st.secrets["GOOGLE_CLIENT_SECRET"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [st.secrets["REDIRECT_URI"]],
        }
    }
    
    # Workaround for environments where from_client_secrets_dict might fail.
    # Write the secrets to a temporary file and load from there.
    secrets_file = "temp_google_secrets.json"
    with open(secrets_file, "w") as f:
        json.dump(client_secrets, f)
        
    flow = Flow.from_client_secrets_file(secrets_file, scopes=SCOPES, redirect_uri=st.secrets["REDIRECT_URI"])
    
    # Clean up the temporary file
    os.remove(secrets_file)
    
    return flow

@st.cache_resource
def gspread_client_from_creds(_credentials):
    """Creates a gspread client from user credentials."""
    return gspread.authorize(_credentials)

def show_login_button():
    """Shows the Google Login button and cookie consent."""
    flow = get_google_flow()
    authorization_url, _ = flow.authorization_url(prompt='consent')
    st.markdown(f'<h1>{l.get("main_menu_title")}</h1>', unsafe_allow_html=True)
    st.markdown(l.get('login_prompt', "Please log in with your Google account to continue."))
    st.link_button("Login with Google", authorization_url, width='stretch')
    st.markdown(l.get('cookie_consent_message', ""), unsafe_allow_html=True) # New consent message

def show_logout_button(cookies_manager): # cookies_manager added as argument
    """Shows user info and a logout button."""
    st.sidebar.divider()
    if 'user_info' in st.session_state:
        user_info = st.session_state['user_info']
        st.sidebar.markdown(f"Logged in as: **{user_info.get('name', 'N/A')}**")
        st.sidebar.markdown(user_info.get('email', ''))
    
    if st.sidebar.button(l.get('btn_logout', 'Logout'), width='stretch'):
        for key in ['creds', 'user_info', 'gspread_client']:
            if key in st.session_state:
                del st.session_state[key]
        cookies_manager['refresh_token'] = '' # Overwrite cookie with empty string
        cookies_manager.save() # Persist the change
        st.rerun()

# --- Core App Logic ---

def check_exp_code_in_sheet(client, sheet_url, exp_code, worksheet_name):
    """Checks if an exp_code already exists in a specific worksheet."""
    if not exp_code:
        return False, "實驗編號不得為空 (Experimental code cannot be empty.)"
    try:
        spreadsheet = client.open_by_url(sheet_url)
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            return True, f"✅ 實驗編號 '{exp_code}' 可用 (分頁 '{worksheet_name}' 尚不存在)。"

        header = sheet.row_values(1)
        if 'exp_code' not in header:
            return True, "警告：在目標分頁中找不到 'exp_code' 欄位，無法檢查唯一性。"
        
        exp_code_col_index = header.index('exp_code') + 1
        existing_codes = sheet.col_values(exp_code_col_index)[1:]
        
        if exp_code in existing_codes:
            return False, f"❌ 實驗編號 '{exp_code}' 已在分頁 '{worksheet_name}' 中存在。"
        else:
            return True, f"✅ 實驗編號 '{exp_code}' 在分頁 '{worksheet_name}' 中可用。"
    except gspread.exceptions.SpreadsheetNotFound:
        return True, "警告：找不到指定的 Google Sheet，無法檢查唯一性。"
    except Exception as e:
        return False, f"🚨 檢查實驗編號時發生錯誤: {e}"

def append_to_sheet(client, sheet_url, data_dict, worksheet_name):
    """
    Appends a dictionary of data as a new row to a specific worksheet,
    ensuring the 'exp_code' is unique within that worksheet.
    Returns:
        (bool, str): A tuple containing a success boolean and a message.
    """
    try:
        spreadsheet = client.open_by_url(sheet_url)
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # Create the worksheet if it doesn't exist
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="50")

        # Uniqueness check for exp_code
        exp_code_to_check = data_dict.get('exp_code')
        if exp_code_to_check:
            header = sheet.row_values(1)
            if header and 'exp_code' in header:
                exp_code_col_index = header.index('exp_code') + 1
                existing_codes = sheet.col_values(exp_code_col_index)[1:]
                if exp_code_to_check in existing_codes:
                    msg = f"❌ 提交失敗：實驗編號 '{exp_code_to_check}' 已在分頁 '{worksheet_name}' 中存在。"
                    return False, msg

        # Flatten data and append
        flat_data = {key: json.dumps(value, ensure_ascii=False) if isinstance(value, list) else value for key, value in data_dict.items()}
        df = pd.DataFrame([flat_data])
        
        existing_headers = sheet.row_values(1)
        if not existing_headers:
            sheet.update([df.columns.values.tolist()] + df.values.tolist())
        else:
            # Ensure all columns from the new data are present in the sheet
            new_cols = [col for col in df.columns if col not in existing_headers]
            if new_cols:
                # Append new columns to the sheet
                sheet.update(f'{gspread.utils.rowcol_to_a1(1, len(existing_headers) + 1)}', [new_cols])
                existing_headers.extend(new_cols)

            df_ordered = pd.DataFrame(columns=existing_headers)
            df_ordered = pd.concat([df_ordered, df], ignore_index=True)
            values_to_append = df_ordered.fillna('').values.tolist()
            sheet.append_rows(values_to_append)
        
        return True, l.get('submit_success', "✅ Data successfully written to Google Sheet!")

    except gspread.exceptions.SpreadsheetNotFound:
        return False, f"{l.get('gcp_sheet_not_found_error')}: {sheet_url}"
    except Exception as e:
        return False, f"{l.get('submit_gspread_error')}: {e}"

# --- Dynamic Page Rendering ---


def _get_cookies_manager():
    return EncryptedCookieManager(
        prefix="labdatasheet-",
        password=st.secrets.get("COOKIE_ENCRYPTION_KEY", "default-insecure-key") # Use a default key for local dev if not set
    )

def _render_dynamic_field(field_def, form_data_key, selected_experimenter, template_name, config):
    """Helper to render a single field from a template definition."""
    key = field_def['key']
    label = l.get(field_def['label_key'], key) 
    optional_tag = l.get('optional_tag') if field_def.get('optional') else ""
    
    # Handle autofill fields
    if field_def['type'] == 'autofill':
        if key == 'date':
            st.session_state[form_data_key][key] = datetime.date.today().strftime('%Y-%m-%d')
        elif key == 'investigator':
            st.session_state[form_data_key][key] = selected_experimenter
        elif key == 'topic':
            st.session_state[form_data_key][key] = template_name
        st.markdown(f"**{label}**: `{st.session_state[form_data_key].get(key, '')}`")
        return

    # Get the current value from session state or default
    current_value = st.session_state[form_data_key].get(key, field_def.get('default'))

    # Render widget
    widget = None
    if key == 'exp_code':
        col1, col2 = st.columns([3, 1])
        with col1:
            widget = st.text_input(
                label + optional_tag, 
                value=current_value or '', 
                key=f"{form_data_key}_{key}",
                placeholder=l.get('ms_exp_code_placeholder', "Enter a unique experimental code")
            )
        with col2:
            st.write("") # V-align
            if st.button(l.get('ms_check_exp_code_button', "Check Code"), width='stretch', key=f"{form_data_key}_check_btn"):
                sheet_url = config.get('google_sheet_url', '')
                if not sheet_url:
                    st.warning(l.get('batch_export_url_missing', "Please configure the sheet URL in Settings."))
                else:
                    with st.spinner("Checking..."):
                        is_unique, message = check_exp_code_in_sheet(st.session_state.gspread_client, sheet_url, widget, template['type'])
                        st.session_state[f'{form_data_key}_check_message'] = message
                        st.session_state[f'{form_data_key}_is_unique'] = is_unique
        
        if f'{form_data_key}_check_message' in st.session_state:
            if st.session_state.get(f'{form_data_key}_is_unique'):
                st.success(st.session_state[f'{form_data_key}_check_message'])
            else:
                st.error(st.session_state[f'{form_data_key}_check_message'])

    elif field_def['type'] == 'text':
        widget = st.text_input(label + optional_tag, value=current_value or '', key=f"{form_data_key}_{key}", placeholder=field_def.get('default', ''))
    elif field_def['type'] == 'number':
        widget = st.number_input(label + optional_tag, value=current_value, key=f"{form_data_key}_{key}", format=field_def.get('format'), step=field_def.get('step'))
    elif field_def['type'] == 'textarea':
        widget = st.text_area(label + optional_tag, value=current_value or '', key=f"{form_data_key}_{key}", placeholder=field_def.get('default', ''))
    elif field_def['type'] == 'radio':
        widget = st.radio(label + optional_tag, options=field_def['options'], index=field_def['options'].index(current_value) if current_value in field_def['options'] else 0, key=f"{form_data_key}_{key}", horizontal=True)

    if widget is not None:
        st.session_state[form_data_key][key] = widget

def _render_dynamic_table(table_def, form_data_key):
    """Helper to render a single table from a template definition."""
    key = table_def['key']
    label = l.get(table_def['label_key'], key.replace('_', ' ').title())
    st.markdown(f"**{label}**")

    # Configure columns for st.data_editor
    column_config = {}
    for col in table_def['columns']:
        col_key = col['key']
        col_label = l.get(col['label_key'], col_key)
        if col['type'] == 'number':
            column_config[col_key] = st.column_config.NumberColumn(col_label, format=col.get('format'))
        else: # text
            column_config[col_key] = st.column_config.TextColumn(col_label, required=col.get('required', False))

    # Initialize or get the DataFrame from session state
    editor_key = f"{form_data_key}_{key}_editor"
    if editor_key not in st.session_state:
        st.session_state[editor_key] = pd.DataFrame(columns=[c['key'] for c in table_def['columns']])

    # Render the data editor
    edited_df = st.data_editor(
        st.session_state[editor_key],
        num_rows="dynamic",
        key=f"{form_data_key}_{key}_widget",
        column_config=column_config,
        width='stretch'
    )
    st.session_state[editor_key] = edited_df
    st.session_state[form_data_key][key] = edited_df.to_dict('records')

def render_dynamic_page(template, form_data_key, selected_experimenter, config):
    """
    Renders a dynamic form page based on a template definition from config.json.
    """
    # Initialize form data in session state if not present
    if form_data_key not in st.session_state:
        st.session_state[form_data_key] = {}
        # Clear any previous check messages
        if f'{form_data_key}_check_message' in st.session_state:
            del st.session_state[f'{form_data_key}_check_message']
    
    # Create a mapping from field key to field definition for quick lookup
    fields_map = {f['key']: f for f in template.get('fields', [])}
    tables_map = {t['key']: t for t in template.get('tables', [])}

    # Iterate through the layout and render UI elements
    for item in template.get('layout', []):
        item_type = item.get('type')
        item_key = item.get('key')
        
        if item_type == 'subheader':
            st.subheader(l.get(item['label_key'], "Subheader"))
        elif item_type == 'divider':
            st.divider()
        elif item_type == 'field' and item_key in fields_map:
            _render_dynamic_field(fields_map[item_key], form_data_key, selected_experimenter, template['name'], config)
        elif item_type == 'table' and item_key in tables_map:
            _render_dynamic_table(tables_map[item_key], form_data_key)

# --- Page Definitions ---


def page_dynamic_template_runner(config, template):
    """
    A generic runner for any dynamic template. It sets up the page and calls the renderer.
    """
    page_title = l.get(f"{template['type']}_page_title", template['name'])
    st.title(page_title)
    form_data_key = f"{template['type']}_form_data"

    if st.button(l.get('btn_back_to_menu')):
        st.session_state.page = 'main_menu'
        if form_data_key in st.session_state:
            del st.session_state[form_data_key]
        st.rerun()
    st.divider()

    selected_experimenter = st.sidebar.selectbox(l.get('sidebar_experimenter_select'), config['experimenters'], key=f"{template['type']}_exp_selector")
    
    st.markdown(f"**{l.get('experiment_label')}** `{template['name']}`")
    st.markdown(f"**{l.get('operator_label')}** `{selected_experimenter}`")
    st.divider()

    # Use the dynamic renderer to build the page
    render_dynamic_page(template, form_data_key, selected_experimenter, config)

    st.divider()

    # --- Submit and Export Buttons ---
    col1, col2 = st.columns(2)
    with col1:
        if st.button(l.get('submit_button'), width='stretch', type="primary"):
            final_form_data = st.session_state.get(form_data_key, {}).copy()

            sheet_url = config.get('google_sheet_url', '')
            if not sheet_url or 'YOUR_GOOGLE_SHEET_URL_HERE' in sheet_url:
                st.warning(l.get('gcp_url_not_set_warning'))
            else:
                with st.spinner(l.get('submit_spinner_writing')):
                    success, message = append_to_sheet(st.session_state.gspread_client, sheet_url, final_form_data, template['type'])
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

    with col2:
        if st.session_state.get(form_data_key):
            st.download_button(
                l.get('export_download_button'),
                data=export_to_word(st.session_state.get(form_data_key, {}), template['name'], l).getvalue(),
                file_name=f"{template['name']}_{st.session_state[form_data_key].get('investigator', 'unknown')}_{st.session_state[form_data_key].get('date', 'nodate')}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                width='stretch'
            )

def page_settings(config):
    st.title(l.get('settings_page_title', "Settings"))
    if st.button(l.get('btn_back_to_menu')):
        # Clear session state for new URL if user navigates away without saving
        if 'new_sheet_url' in st.session_state:
            del st.session_state.new_sheet_url
        st.session_state.page = 'main_menu'
        st.rerun()
    st.divider()

    # --- Google Sheet URL Settings ---
    st.subheader(l.get('settings_gsheet_subheader', "Google Sheet Settings"))
    
    # Check session state for a newly created URL, otherwise use config
    default_url = st.session_state.get('new_sheet_url', config.get('google_sheet_url', ''))
    
    new_gsheet_url = st.text_input(
        l.get('settings_gsheet_label', "Target Google Sheet URL"),
        value=default_url,
        help=l.get('settings_gsheet_help', "Paste the URL of the Google Sheet where data will be appended.")
    )

    if st.button(l.get('settings_gsheet_create_button', "Create a new Google Sheet for me")):
        try:
            with st.spinner(l.get('settings_gsheet_create_spinner', "Creating new Google Sheet...")):
                spreadsheet = st.session_state.gspread_client.create('Lab Data Sheet (Generated by App)')
                st.session_state.new_sheet_url = spreadsheet.url
            st.success(l.get('settings_gsheet_create_success', "Success! New sheet created in your Google Drive. Save changes to use it."))
            st.rerun()
        except Exception as e:
            st.error(l.get('settings_gsheet_create_error', 'Error creating Google Sheet'))
            st.exception(e)

    st.divider()

    # --- Operator Settings ---
    st.subheader(l.get('settings_operator_subheader', "Manage Operators"))
    current_experimenters = "\n".join(config.get('experimenters', []))
    new_experimenters_str = st.text_area(
        l.get('settings_operator_label', "Operators (one per line)"),
        value=current_experimenters, height=200,
        help=l.get('settings_operator_help', "...")
    )

    # --- Save Button ---
    if st.button(l.get('settings_save_button', "Save Changes"), type="primary", width='stretch'):
        # Update experimenters
        new_list = [name.strip() for name in new_experimenters_str.split('\n') if name.strip()]
        config['experimenters'] = new_list
        
        # Update Google Sheet URL
        config['google_sheet_url'] = new_gsheet_url

        try:
            save_config('config.json', config)
            # Clear session state for new URL after saving
            if 'new_sheet_url' in st.session_state:
                del st.session_state.new_sheet_url
            st.success(l.get('settings_save_success', "Settings saved successfully!"))
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"{l.get('settings_save_error', 'Error saving settings')}: {e}")

def main_menu():
    st.title(l.get('main_menu_title'))
    st.subheader(l.get('main_menu_subtitle'))
    st.markdown("---")
    
    # Dynamically display buttons for each template
    for template in load_config('config.json')['templates']:
        if st.button(l.get(template['button_label_key'], template['name']), width='stretch'):
            st.session_state.page = template['type']
            st.rerun()

    st.markdown("---")
    if st.button(l.get('btn_batch_export', '📦 Batch Export to Word'), width='stretch'):
        st.session_state.page = 'batch_export'
        st.rerun()

def page_dynamic_template_runner(config, template):
    """
    A generic runner for any dynamic template. It sets up the page and calls the renderer.
    """
    page_title = l.get(f"{template['type']}_page_title", template['name'])
    st.title(page_title)
    form_data_key = f"{template['type']}_form_data"

    if st.button(l.get('btn_back_to_menu')):
        st.session_state.page = 'main_menu'
        if form_data_key in st.session_state:
            del st.session_state[form_data_key]
        st.rerun()
    st.divider()

    selected_experimenter = st.sidebar.selectbox(l.get('sidebar_experimenter_select'), config['experimenters'], key=f"{template['type']}_exp_selector")
    
    st.markdown(f"**{l.get('experiment_label')}** `{template['name']}`")
    st.markdown(f"**{l.get('operator_label')}** `{selected_experimenter}`")
    st.divider()

    # Use the dynamic renderer to build the page
    render_dynamic_page(template, form_data_key, selected_experimenter, config)

    st.divider()

    # --- Submit and Export Buttons ---
    col1, col2 = st.columns(2)
    with col1:
        if st.button(l.get('submit_button'), width='stretch', type="primary"):
            final_form_data = st.session_state.get(form_data_key, {}).copy()

            sheet_url = config.get('google_sheet_url', '')
            if not sheet_url or 'YOUR_GOOGLE_SHEET_URL_HERE' in sheet_url:
                st.warning(l.get('gcp_url_not_set_warning'))
            else:
                with st.spinner(l.get('submit_spinner_writing')):
                    success, message = append_to_sheet(st.session_state.gspread_client, sheet_url, final_form_data, template['type'])
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

    with col2:
        if st.session_state.get(form_data_key):
            st.download_button(
                l.get('export_download_button'),
                data=export_to_word(st.session_state.get(form_data_key, {}), template['name'], l).getvalue(),
                file_name=f"{template['name']}_{st.session_state[form_data_key].get('investigator', 'unknown')}_{st.session_state[form_data_key].get('date', 'nodate')}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                width='stretch'
            )

def page_batch_export(config):
    st.title(l.get('batch_export_page_title', "Batch Export to Word"))
    if st.button(l.get('btn_back_to_menu')):
        st.session_state.page = 'main_menu'
        st.rerun()
    st.divider()

    sheet_url = config.get('google_sheet_url', '')

    st.info(l.get('batch_export_info', "This feature will read all data from all tabs in the configured Google Sheet and generate a Word document for each row, then package them into a single .zip file for you to download."))
    
    if not sheet_url:
        st.warning(l.get('batch_export_url_missing', "⚠️ Please go to the Settings page to configure the target Google Sheet URL first."))
        return

    st.markdown(f"**Source Google Sheet:** `{sheet_url}`")

    if st.button(l.get('batch_export_button', "Generate All Word Documents from Google Sheet"), type="primary", width='stretch'):
        try:
            with st.spinner(l.get('batch_export_spinner', "Reading data and generating Word documents... Please wait.")):
                spreadsheet = st.session_state.gspread_client.open_by_url(sheet_url)
                zip_buffer = io.BytesIO()
                total_docs_generated = 0

                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    # Loop through each template defined in config
                    for template in config['templates']:
                        worksheet_name = template['type']
                        try:
                            sheet = spreadsheet.worksheet(worksheet_name)
                        except gspread.exceptions.WorksheetNotFound:
                            continue # Skip if tab doesn't exist

                        records = sheet.get_all_records()
                        if not records:
                            continue

                        # Loop through each record in the current worksheet
                        for i, record in enumerate(records):
                            reconstructed_record = record.copy()
                            for key, value in reconstructed_record.items():
                                if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                                    try:
                                        reconstructed_record[key] = json.loads(value)
                                    except json.JSONDecodeError:
                                        pass
                            
                            file_stream = export_to_word(reconstructed_record, template['name'], l)
                            
                            filename = f"{worksheet_name}/{reconstructed_record.get('date', 'nodate')}_{reconstructed_record.get('exp_code', f'row_{i+2}')}.docx"
                            zf.writestr(filename, file_stream.getvalue())
                            total_docs_generated += 1

                if total_docs_generated > 0:
                    st.session_state.zip_buffer = zip_buffer
                    st.session_state.zip_filename = f"BatchExport_{datetime.date.today().strftime('%Y-%m-%d')}.zip"
                    st.success(l.get('batch_export_success', "✅ Successfully generated {count} Word documents.").format(count=total_docs_generated))
                else:
                    st.warning(l.get('batch_export_no_data', "No data found in any worksheet to export."))

        except Exception as e:
            st.error(l.get('batch_export_error', "🚨 An error occurred during the process."))
            st.exception(e)

    if 'zip_buffer' in st.session_state and 'zip_filename' in st.session_state:
        st.download_button(
            label=l.get('batch_export_download_button', "Download Zip File with all Word Documents"),
            data=st.session_state.zip_buffer.getvalue(),
            file_name=st.session_state.zip_filename,
            mime="application/zip",
            width='stretch',
            on_click=lambda: (st.session_state.pop('zip_buffer', None), st.session_state.pop('zip_filename', None))
        )

# --- Main App Router ---
def main():
    # --- Initialize Cookie Manager ---
    cookies_manager = _get_cookies_manager()
    if not cookies_manager.ready():
        st.stop() # Wait for cookies to be ready

    # Handle OAuth2 callback
    query_params = st.query_params
    if 'code' in query_params and 'creds' not in st.session_state:
        try:
            flow = get_google_flow()
            flow.fetch_token(code=query_params['code'])
            st.session_state['creds'] = flow.credentials
            # Get user info
            from google.oauth2 import id_token
            id_info = id_token.verify_oauth2_token(st.session_state['creds'].id_token, requests.Request(), st.secrets["GOOGLE_CLIENT_ID"])
            st.session_state['user_info'] = id_info
            
            # --- Save refresh token to cookie ---
            if st.session_state['creds'].refresh_token:
                cookies_manager['refresh_token'] = st.session_state['creds'].refresh_token
                cookies_manager.save()
            
            # Clear query params
            st.query_params.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Error during login:")
            st.exception(e)
            st.stop()

    # --- Auto-login from cookie ---
    if 'creds' not in st.session_state:
        refresh_token_from_cookie = cookies_manager.get('refresh_token')
        if refresh_token_from_cookie:
            try:
                # Build dummy creds with refresh token
                creds = Credentials(None,
                                    refresh_token=refresh_token_from_cookie,
                                    token_uri="https://oauth2.googleapis.com/token", # Hardcoded for now
                                    client_id=st.secrets["GOOGLE_CLIENT_ID"],
                                    client_secret=st.secrets["GOOGLE_CLIENT_SECRET"])
                creds.refresh(requests.Request()) # Refresh to get new access token
                st.session_state['creds'] = creds
                
                # Get user info for display
                from google.oauth2 import id_token
                id_info = id_token.verify_oauth2_token(creds.id_token, requests.Request(), st.secrets["GOOGLE_CLIENT_ID"])
                st.session_state['user_info'] = id_info
                
                st.rerun() # Rerun to update UI as logged in
            except Exception as e:
                st.warning(f"自動登入失敗，請重新登入:")
                st.exception(e) # Print full traceback for debugging
                del cookies_manager['refresh_token'] # Clear invalid cookie
                cookies_manager.save()

    # If not logged in, show login button
    if 'creds' not in st.session_state:
        show_login_button()
        st.stop()

    # --- Main App (if logged in) ---
    if 'gspread_client' not in st.session_state:
        st.session_state['gspread_client'] = gspread_client_from_creds(st.session_state['creds'])

    if 'page' not in st.session_state:
        st.session_state.page = 'main_menu'
    
    show_logout_button(cookies_manager) # Pass cookies_manager
    st.sidebar.title(l.get('sidebar_setup_title'))
    if st.sidebar.button(l.get('btn_settings', "Settings"), width='stretch'):
        st.session_state.page = 'settings'
        st.rerun()
    
    config = load_config('config.json')

    # Dynamically build PAGES dictionary
    PAGES = {
        'main_menu': main_menu,
        'batch_export': lambda: page_batch_export(config),
        'settings': lambda: page_settings(config),
    }
    
    for template in config['templates']:
        if template['type'] not in PAGES:
            PAGES[template['type']] = lambda t=template: page_dynamic_template_runner(config, t)

    page_function = PAGES.get(st.session_state.page, main_menu)
    page_function()

if __name__ == "__main__":
    main()