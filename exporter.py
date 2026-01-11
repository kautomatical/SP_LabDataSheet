import io
from docxtpl import DocxTemplate
import datetime

def export_to_word(data, template_name, lang_dict):
    """
    Generates a Word document by rendering a template with experiment data.

    Args:
        data (dict): A dictionary containing the experiment data. 
                     The keys are the internal keys from config.json (e.g., 'exp_code'),
                     and values are the data points.
        template_name (str): The name of the template (e.g., 'Electrochemical_Testing').
        lang_dict (dict): A dictionary containing localized strings (not used by docxtpl but kept for consistency).

    Returns:
        io.BytesIO: An in-memory binary stream containing the Word document.
    """
    # Construct the template path from the name
    # e.g., 'Electrochemical_Testing' -> 'template_Electrochemical_Testing.docx'
    template_path = f"template_{template_name}.docx"

    try:
        doc = DocxTemplate(template_path)
    except Exception as e:
        # This will fail if the template file doesn't exist, which is expected for now.
        # We will instruct the user to create it later.
        # In a real app, you'd have better error handling here.
        print(f"Template file not found at {template_path}. Error: {e}")
        # Create a dummy empty stream to avoid crashing the app
        file_stream = io.BytesIO()
        # We can't use the docx library to write an error easily without it being a dependency.
        # So we just return an empty doc. The user will be notified in the UI.
        return file_stream

    # The context keys must match the {{tags}} in the .docx template
    # The `data` dict already uses the 'key' from config.json, so we can pass it directly.
    context = data.copy()

    # The user will need to create a template with these exact tags, e.g., {{cathode_table}}
    # docxtpl can iterate through a list of dicts to create table rows.
    # We assume the data for tables is passed in `data` with specific keys.
    # For example, data['cathode_table'] = [{'type': 'Li', 'weight': 10}, ...]
    
    doc.render(context)

    # Save the document to an in-memory stream
    file_stream = io.BytesIO()
    doc.save(file_stream)
    file_stream.seek(0)
    
    return file_stream

