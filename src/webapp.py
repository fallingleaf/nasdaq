"""
Masonias Web Application
========================

Flask web application providing:
1. Database Query Tool - Execute read-only SQL queries on market data
2. Services Manager - Run data pipeline tasks (fetch prices, generate reports, compute SMA events)
3. Landing Page - Beautiful homepage for Masonias.com

Author: Masonias Team
"""

# ============================================================================
# IMPORTS
# ============================================================================

# Flask and web framework
from flask import Flask, Blueprint, render_template, request, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix

# Database
import sqlalchemy
from db import load_database_config, create_engine_from_config

# Standard library
import re
import os
import threading
import sys
import io
import traceback
import yaml
from datetime import datetime, date
from collections import deque
from contextlib import redirect_stdout, redirect_stderr

# Service modules (data pipeline scripts)
import all_prices
import generate_report
import thirty_day_report
import sma_events

# ============================================================================
# APPLICATION SETUP
# ============================================================================

app = Flask(__name__)

# Create blueprints for organizing routes
db_bp = Blueprint('db', __name__, url_prefix='/db')
services_bp = Blueprint('services', __name__, url_prefix='/services')

# Configure for running behind a reverse proxy (nginx)
app.config['APPLICATION_ROOT'] = os.getenv('APPLICATION_ROOT', '/')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

# Initialize database connection
db_config = load_database_config()
engine = create_engine_from_config(db_config)

# ============================================================================
# API KEYS CONFIGURATION
# ============================================================================

# Load full configuration file for API keys
config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
full_config = {}

if os.path.exists(config_path):
    with open(config_path, 'r') as f:
        full_config = yaml.safe_load(f) or {}

# Set environment variables from config if not already set
# This allows services to access API keys without passing them explicitly
if 'polygon' in full_config and 'api_key' in full_config['polygon']:
    if not os.getenv('POLYGON_API_KEY'):
        os.environ['POLYGON_API_KEY'] = full_config['polygon']['api_key']

if 'openai' in full_config and 'api_key' in full_config['openai']:
    if not os.getenv('OPENAI_API_KEY'):
        os.environ['OPENAI_API_KEY'] = full_config['openai']['api_key']

# ============================================================================
# JOB TRACKING (for background service execution)
# ============================================================================

# Global job tracking storage
jobs = {}  # Dict mapping job_id -> job_info
job_counter = 0  # Auto-incrementing counter for job IDs
job_lock = threading.Lock()  # Thread lock for safe concurrent access

# Constants
MAX_LOG_LINES = 1000  # Maximum number of log lines to keep per job
MAX_COMPLETED_JOBS = 10  # Maximum number of completed jobs to keep in memory
VALID_SERVICES = [
    'fetch_prices',        # Fetch stock prices from Polygon API
    'daily_report',        # Generate daily market report
    'thirty_day_report',   # Generate 30-day market summary
    'compute_sma_events'   # Compute SMA crossover events
]

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def is_read_only_query(query: str) -> bool:
    """
    Validate that a SQL query is read-only (SELECT only).

    This security function prevents destructive operations by:
    1. Stripping SQL comments
    2. Checking the query starts with SELECT
    3. Blocking dangerous keywords (INSERT, UPDATE, DELETE, etc.)

    Args:
        query: SQL query string to validate

    Returns:
        True if query is safe (read-only), False otherwise
    """
    # Remove SQL comments (both -- and /* */ styles)
    query_normalized = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    query_normalized = re.sub(r'/\*.*?\*/', '', query_normalized, flags=re.DOTALL)
    query_normalized = query_normalized.strip().upper()

    # Must start with SELECT
    if not query_normalized.startswith('SELECT'):
        return False

    # Block any dangerous keywords that could modify data
    dangerous_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
        'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE'
    ]

    for keyword in dangerous_keywords:
        # Use word boundaries to avoid false positives
        if re.search(r'\b' + keyword + r'\b', query_normalized):
            return False

    return True

def cleanup_old_jobs():
    """
    Clean up old completed jobs to prevent memory overflow.

    Keeps all running jobs and only the MAX_COMPLETED_JOBS most recent
    completed/failed jobs. Should be called after each job completion.

    This function is thread-safe and uses the job_lock.
    """
    with job_lock:
        # Separate running and completed jobs
        running_jobs = {}
        completed_jobs = []

        for job_id, job in jobs.items():
            if job['status'] == 'running':
                running_jobs[job_id] = job
            else:
                completed_jobs.append((job_id, job))

        # Sort completed jobs by end_time (newest first)
        # Jobs without end_time go to the end
        completed_jobs.sort(
            key=lambda x: x[1].get('end_time', ''),
            reverse=True
        )

        # Keep only the most recent MAX_COMPLETED_JOBS completed jobs
        jobs_to_keep = dict(completed_jobs[:MAX_COMPLETED_JOBS])

        # Update global jobs dict with running + recent completed jobs
        jobs.clear()
        jobs.update(running_jobs)
        jobs.update(jobs_to_keep)

        # Log cleanup if jobs were removed
        removed_count = len(completed_jobs) - len(jobs_to_keep)
        if removed_count > 0:
            print(f"Cleaned up {removed_count} old job(s). "
                  f"Keeping {len(running_jobs)} running + {len(jobs_to_keep)} completed jobs.")

# ============================================================================
# DATABASE QUERY BLUEPRINT - Routes for /db/*
# ============================================================================

@db_bp.route('/')
def index():
    """Render the database query interface page."""
    return render_template('db.html')

@db_bp.route('/query', methods=['POST'])
def execute_query():
    """
    Execute a read-only SQL query and return results.

    POST body (JSON):
        {
            "query": "SELECT * FROM companies LIMIT 10"
        }

    Returns:
        JSON with columns, rows, and row_count
    """
    try:
        query = request.json.get('query', '').strip()

        # Validate query is not empty
        if not query:
            return jsonify({'error': 'Query cannot be empty'}), 400

        # Security: Validate query is read-only
        if not is_read_only_query(query):
            return jsonify({
                'error': 'Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, etc.'
            }), 400

        # Execute query against database
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query))

            # Extract column names
            columns = list(result.keys())

            # Convert rows to list of dictionaries
            rows = []
            for row in result:
                rows.append(dict(zip(columns, row)))

            return jsonify({
                'columns': columns,
                'rows': rows,
                'row_count': len(rows)
            })

    except sqlalchemy.exc.SQLAlchemyError as e:
        # Database-specific errors
        return jsonify({'error': f'Database error: {str(e)}'}), 400
    except Exception as e:
        # General errors
        return jsonify({'error': f'Error: {str(e)}'}), 500

@db_bp.route('/tables')
def get_tables():
    """
    Get list of all available database tables.

    Returns:
        JSON with list of table names
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SHOW TABLES"))
            tables = [row[0] for row in result]
            return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@db_bp.route('/schema/<table_name>')
def get_schema(table_name):
    """
    Get schema (column definitions) for a specific table.

    Args:
        table_name: Name of the table to describe

    Returns:
        JSON with column information (field, type, null, key, default, extra)
    """
    try:
        # Security: Validate table name contains only alphanumeric and underscore
        # This prevents SQL injection attacks
        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
            return jsonify({'error': 'Invalid table name'}), 400

        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"DESCRIBE {table_name}"))
            columns = []
            for row in result:
                columns.append({
                    'field': row[0],
                    'type': row[1],
                    'null': row[2],
                    'key': row[3],
                    'default': row[4],
                    'extra': row[5]
                })
            return jsonify({'columns': columns})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# SERVICES BLUEPRINT - Routes for /services/*
# ============================================================================

@services_bp.route('/')
def services():
    """Render the services management page."""
    return render_template('services.html')

@services_bp.route('/run', methods=['POST'])
def run_service():
    """
    Start a background service job.

    POST body (JSON):
        {
            "service": "fetch_prices",
            "params": {"date": "2024-01-01"}
        }

    Returns:
        JSON with job_id, status, and message
    """
    global job_counter

    try:
        data = request.json
        service_name = data.get('service')
        params = data.get('params', {})

        # Validate service name
        if service_name not in VALID_SERVICES:
            return jsonify({'error': f'Invalid service name. Valid options: {VALID_SERVICES}'}), 400

        # Create job entry with thread-safe locking
        with job_lock:
            job_counter += 1
            job_id = f"job_{job_counter}"
            jobs[job_id] = {
                'id': job_id,
                'service': service_name,
                'params': params,
                'status': 'running',
                'start_time': datetime.now().isoformat(),
                'end_time': None,
                'logs': deque(maxlen=MAX_LOG_LINES),  # Circular buffer for logs
                'exit_code': None
            }

        # Start background thread to execute the job
        # Daemon thread will automatically terminate when main program exits
        thread = threading.Thread(target=execute_job, args=(job_id, service_name, params))
        thread.daemon = True
        thread.start()

        return jsonify({
            'job_id': job_id,
            'status': 'running',
            'message': f'Job {job_id} started successfully'
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@services_bp.route('/jobs')
def list_jobs():
    """
    List recent jobs (running, completed, failed).

    Returns most recent 10 jobs to keep UI clean and performant.

    Returns:
        JSON with list of jobs (without logs)
    """
    with job_lock:
        job_list = []
        for job_id, job in jobs.items():
            # Return job info without logs (logs can be large)
            job_list.append({
                'id': job['id'],
                'service': job['service'],
                'params': job['params'],
                'status': job['status'],
                'start_time': job['start_time'],
                'end_time': job['end_time'],
                'exit_code': job['exit_code']
            })

        # Sort by start time (newest first) and limit to 10
        job_list.sort(key=lambda x: x['start_time'], reverse=True)
        return jsonify({'jobs': job_list[:10]})

@services_bp.route('/jobs/<job_id>')
def get_job_status(job_id):
    """
    Get detailed status and logs for a specific job.

    Args:
        job_id: Job identifier (e.g., "job_1")

    Returns:
        JSON with complete job information including logs
    """
    with job_lock:
        if job_id not in jobs:
            return jsonify({'error': 'Job not found'}), 404

        job = jobs[job_id]
        return jsonify({
            'id': job['id'],
            'service': job['service'],
            'params': job['params'],
            'status': job['status'],
            'start_time': job['start_time'],
            'end_time': job['end_time'],
            'exit_code': job['exit_code'],
            'logs': list(job['logs'])  # Convert deque to list for JSON serialization
        })

# ============================================================================
# JOB EXECUTION ENGINE
# ============================================================================

class LogCapture(io.StringIO):
    """
    Custom output stream that captures stdout/stderr and adds to job logs.

    This class intercepts print statements and logging output from service
    scripts and stores them in the job's log buffer line by line.
    """

    def __init__(self, job_id):
        """
        Initialize the log capture stream.

        Args:
            job_id: Job identifier to associate logs with
        """
        super().__init__()
        self.job_id = job_id
        self.buffer = ""  # Buffer for incomplete lines

    def write(self, s):
        """
        Capture output and split into lines.

        Args:
            s: String to write (may contain multiple lines)

        Returns:
            Number of characters written
        """
        self.buffer += s

        # Process complete lines
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            if line:  # Skip empty lines
                with job_lock:
                    if self.job_id in jobs:
                        jobs[self.job_id]['logs'].append(line)

        return len(s)

    def flush(self):
        """Flush any remaining buffered output to logs."""
        if self.buffer:
            with job_lock:
                if self.job_id in jobs:
                    jobs[self.job_id]['logs'].append(self.buffer)
            self.buffer = ""

def execute_job(job_id, service_name, params):
    """
    Execute a service job in background thread.

    This function:
    1. Validates prerequisites (e.g., API keys)
    2. Builds command-line arguments
    3. Redirects stdout/stderr to capture logs
    4. Calls the service's main() function
    5. Updates job status on completion/failure

    Args:
        job_id: Job identifier
        service_name: Name of service to run
        params: Dictionary of parameters for the service
    """
    try:
        # Log job start
        with job_lock:
            jobs[job_id]['logs'].append(f"Starting service: {service_name}")
            jobs[job_id]['logs'].append(f"Parameters: {params}")

        # Validate prerequisites: Check for required API keys
        if service_name == 'fetch_prices' and not os.getenv('POLYGON_API_KEY'):
            with job_lock:
                jobs[job_id]['status'] = 'failed'
                jobs[job_id]['exit_code'] = 1
                jobs[job_id]['end_time'] = datetime.now().isoformat()
                jobs[job_id]['logs'].append("ERROR: Polygon API key not configured!")
                jobs[job_id]['logs'].append("Please add your API key to config.yaml under polygon.api_key")
            return

        # Prepare output capture stream
        output_capture = LogCapture(job_id)

        # Save original sys.argv to restore later
        original_argv = sys.argv.copy()

        try:
            # Build command-line arguments for the service
            # Each service uses argparse and expects sys.argv
            sys.argv = [service_name]

            # Add service-specific parameters
            if service_name == 'fetch_prices':
                if 'date' in params and params['date']:
                    sys.argv.extend(['--date', params['date']])

            elif service_name == 'daily_report':
                if 'report_date' in params and params['report_date']:
                    sys.argv.extend(['--report-date', params['report_date']])

            elif service_name == 'thirty_day_report':
                if 'report_date' in params and params['report_date']:
                    sys.argv.extend(['--report-date', params['report_date']])

            elif service_name == 'compute_sma_events':
                if 'short_window' in params and params['short_window']:
                    sys.argv.extend(['--short-window', str(params['short_window'])])
                if 'long_window' in params and params['long_window']:
                    sys.argv.extend(['--long-window', str(params['long_window'])])

            # Execute the service with output redirection
            # All print() and logging calls will be captured
            with redirect_stdout(output_capture), redirect_stderr(output_capture):
                if service_name == 'fetch_prices':
                    all_prices.main()
                elif service_name == 'daily_report':
                    generate_report.main()
                elif service_name == 'thirty_day_report':
                    thirty_day_report.main()
                elif service_name == 'compute_sma_events':
                    sma_events.main()

            # Ensure all buffered output is written
            output_capture.flush()

            # Mark job as successfully completed
            with job_lock:
                jobs[job_id]['status'] = 'completed'
                jobs[job_id]['exit_code'] = 0
                jobs[job_id]['end_time'] = datetime.now().isoformat()
                jobs[job_id]['logs'].append(f"Service completed successfully")

            # Clean up old jobs after completion
            cleanup_old_jobs()

        finally:
            # Always restore original sys.argv
            sys.argv = original_argv

    except SystemExit as e:
        """
        Handle sys.exit() calls from service scripts.
        Exit code 0 = success, non-zero = failure
        """
        output_capture.flush()
        exit_code = e.code if e.code is not None else 0

        with job_lock:
            jobs[job_id]['status'] = 'completed' if exit_code == 0 else 'failed'
            jobs[job_id]['exit_code'] = exit_code
            jobs[job_id]['end_time'] = datetime.now().isoformat()
            jobs[job_id]['logs'].append(f"Service exited with code {exit_code}")

        # Clean up old jobs after completion
        cleanup_old_jobs()

    except Exception as e:
        """Handle unexpected errors during job execution."""
        output_capture.flush()

        with job_lock:
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['exit_code'] = 1
            jobs[job_id]['end_time'] = datetime.now().isoformat()
            jobs[job_id]['logs'].append(f"Error: {str(e)}")

            # Add full traceback for debugging
            jobs[job_id]['logs'].append("Traceback:")
            jobs[job_id]['logs'].append(traceback.format_exc())

        # Clean up old jobs after failure
        cleanup_old_jobs()

# ============================================================================
# MAIN APPLICATION ROUTES
# ============================================================================

@app.route('/')
def home():
    """Render the landing page (Masonias homepage)."""
    return render_template('home.html')

@app.errorhandler(404)
def page_not_found(e):
    """Render custom 404 error page."""
    return render_template('404.html'), 404

# ============================================================================
# BLUEPRINT REGISTRATION
# ============================================================================

# Register blueprints with the main app
app.register_blueprint(db_bp)
app.register_blueprint(services_bp)

# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    # Run Flask development server
    # In production, use gunicorn or another WSGI server
    app.run(debug=True, host='0.0.0.0', port=5001)
