import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash
from werkzeug.utils import secure_filename
from logic import execute_split, execute_main
import datetime
import random
import string

# --- Configuration ---
UPLOAD_FOLDER = '.' # Base directory
SESSIONS_FOLDER = os.path.join(UPLOAD_FOLDER, 'sessions') # New dedicated folder for all sessions
ALLOWED_EXTENSIONS = {'txt', 'log'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = 'supersecretkey' # Needed for flashing messages

def allowed_file(filename):
    """Checks if the uploaded file has an allowed extension."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    """Handles the file upload and processing logic."""
    if 'logfiles' not in request.files:
        flash('No file part in the request.', 'error')
        return redirect(url_for('index'))

    # --- DYNAMIC SESSION NAME GENERATION ---
    now = datetime.datetime.now()
    date_part = now.strftime("%m%d") # Format: monthday
    random_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    session = f"{date_part}_{random_part}"

    files = request.files.getlist('logfiles')

    if not files or files[0].filename == '':
        flash('No files selected for uploading.', 'error')
        return redirect(url_for('index'))

    # --- Create Session Directories inside the 'sessions' folder ---
    os.makedirs(SESSIONS_FOLDER, exist_ok=True) # Ensure the main sessions folder exists
    session_base_path = os.path.join(SESSIONS_FOLDER, session)
    input_folder = os.path.join(session_base_path, 'input')
    output_folder = os.path.join(session_base_path, 'output')
    split_folder = os.path.join(input_folder, 'split')

    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(split_folder, exist_ok=True)

    # --- Save Uploaded Files ---
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(input_folder, filename))
        else:
            flash(f"File type not allowed for '{file.filename}'. Only .txt and .log files are accepted.", 'error')
            return redirect(url_for('index'))
    
    # --- Run Processing Logic ---
    try:
        # --- VALIDATION CHECK ---
        validation_errors = execute_split(input_folder, split_folder)
        if validation_errors:
            for error in validation_errors:
                flash(error, 'error')
            return redirect(url_for('index'))
        
        # If validation passes, continue to main processing
        execute_main(split_folder, output_folder)

    except Exception as e:
        flash(f"An error occurred during processing: {e}", 'error')
        return redirect(url_for('index'))

    return redirect(url_for('results', session_name=session))

@app.route('/results/<session_name>')
def results(session_name):
    """
    Displays the results page with a summary table from the main Excel file
    and download links for other reports.
    """
    session_path = secure_filename(session_name)
    output_folder = os.path.join(SESSIONS_FOLDER, session_path, 'output')
    
    if not os.path.isdir(output_folder):
        flash('Results folder not found for the given session.', 'error')
        return redirect(url_for('index'))

    table_html = None
    other_files = []
    main_report_name = 'Final_Report.xlsx'
    main_report_path = os.path.join(output_folder, main_report_name)

    if os.path.exists(main_report_path):
        try:
            df = pd.read_excel(main_report_path)
            table_classes = "min-w-full divide-y divide-slate-700"
            table_html = df.to_html(classes=table_classes, border=0, index=False, na_rep='-')
        except Exception as e:
            flash(f"Could not read or process the main report file: {e}", 'error')
    
    try:
        all_files = [f for f in os.listdir(output_folder) if f.endswith('.xlsx')]
        other_files = sorted([f for f in all_files if f != main_report_name])
    except FileNotFoundError:
        flash('Output directory does not exist.', 'error')

    return render_template('results.html', 
                           session_name=session_name,
                           table_html=table_html,
                           other_files=other_files,
                           main_report_name=main_report_name)


@app.route('/download/<session_name>/<filename>')
def download_file(session_name, filename):
    """Handles downloading of a specific result file."""
    session_path = secure_filename(session_name)
    filename_safe = secure_filename(filename)
    output_folder = os.path.join(SESSIONS_FOLDER, session_path, 'output')
    return send_from_directory(directory=output_folder, path=filename_safe, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
