"""
Simple Flask web application for executing read-only database queries.
"""

from flask import Flask, render_template, request, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
import sqlalchemy
from db import load_database_config, create_engine_from_config
import re
import os

app = Flask(__name__)

# Configure for running behind a reverse proxy
app.config['APPLICATION_ROOT'] = os.getenv('APPLICATION_ROOT', '/')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Initialize database engine
db_config = load_database_config()
engine = create_engine_from_config(db_config)

def is_read_only_query(query):
    """
    Check if the query is read-only (SELECT statements only).
    Returns True if safe, False otherwise.
    """
    # Remove comments and normalize whitespace
    query_normalized = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    query_normalized = re.sub(r'/\*.*?\*/', '', query_normalized, flags=re.DOTALL)
    query_normalized = query_normalized.strip().upper()

    # Check if query starts with SELECT
    if not query_normalized.startswith('SELECT'):
        return False

    # Block dangerous keywords
    dangerous_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
        'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE'
    ]

    for keyword in dangerous_keywords:
        if re.search(r'\b' + keyword + r'\b', query_normalized):
            return False

    return True

@app.route('/')
def index():
    """Render the main query interface."""
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def execute_query():
    """Execute a read-only SQL query and return results."""
    try:
        query = request.json.get('query', '').strip()

        if not query:
            return jsonify({'error': 'Query cannot be empty'}), 400

        # Validate read-only query
        if not is_read_only_query(query):
            return jsonify({
                'error': 'Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, etc.'
            }), 400

        # Execute query
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query))

            # Fetch column names
            columns = list(result.keys())

            # Fetch all rows and convert to list of dicts
            rows = []
            for row in result:
                rows.append(dict(zip(columns, row)))

            return jsonify({
                'columns': columns,
                'rows': rows,
                'row_count': len(rows)
            })

    except sqlalchemy.exc.SQLAlchemyError as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': f'Error: {str(e)}'}), 500

@app.route('/tables')
def get_tables():
    """Get list of available tables."""
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SHOW TABLES"))
            tables = [row[0] for row in result]
            return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/schema/<table_name>')
def get_schema(table_name):
    """Get schema for a specific table."""
    try:
        # Validate table name to prevent SQL injection
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
