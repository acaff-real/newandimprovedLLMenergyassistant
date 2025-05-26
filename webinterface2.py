from flask import Flask, request, jsonify
import mysql.connector
import requests
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password1234',  
    'database': 'iexinternetdatacenter',
    'port': 3306
}

LLM_CONFIG = {
    'endpoint': 'http://127.0.0.1:1234',
    'model_name': 'mistral-7b-instruct-v0.3',
    'temperature': 0.1,
    'max_tokens': 500
}

# Global state
db_connection = None
schema_cache = None

# Database functions
def db_connect():
    global db_connection
    try:
        db_connection = mysql.connector.connect(**DB_CONFIG)
        logger.info("Database connection established")
        return True
    except mysql.connector.Error as e:
        logger.error(f"Database connection failed: {e}")
        return False

def db_get_schema():
    global schema_cache, db_connection
    
    if schema_cache:
        return schema_cache
    
    if not db_connection or not db_connection.is_connected():
        if not db_connect():
            raise RuntimeError("Failed to connect to database")
    
    cursor = db_connection.cursor()
    schema_info = []
    
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            schema_info.append(f"\nTable: {table_name}")
            
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            for column in columns:
                col_name, col_type, null, key, default, extra = column
                key_info = f" ({key})" if key else ""
                schema_info.append(f"  - {col_name}: {col_type}{key_info}")
            
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            sample_data = cursor.fetchall()
            if sample_data:
                schema_info.append(f"  Sample data: {sample_data[:2]}")
    
    except mysql.connector.Error as e:
        logger.error(f"Schema extraction failed: {e}")
        raise
    finally:
        cursor.close()
    
    schema_cache = "\n".join(schema_info)
    return schema_cache

def db_execute_query(sql):
    global db_connection
    
    if not db_connection or not db_connection.is_connected():
        if not db_connect():
            return {"success": False, "error": "Database connection failed"}
    
    cursor = db_connection.cursor()
    
    try:
        cursor.execute(sql)
        
        if cursor.description:  # SELECT query
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows)
            }
        else:  # Non-SELECT query
            db_connection.commit()
            return {
                "success": True,
                "affected_rows": cursor.rowcount,
                "message": "Query executed successfully"
            }
    
    except mysql.connector.Error as e:
        logger.error(f"Query execution failed: {e}")
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        cursor.close()

def db_close():
    global db_connection
    if db_connection and db_connection.is_connected():
        db_connection.close()
        logger.info("Database connection closed")

# LLM functions
def llm_generate_sql(natural_query, schema):
    prompt = f"""You are an expert SQL generator for an electricity market database. Convert natural language queries to valid MySQL SQL.

Database Schema:
{schema}

Key columns explained:
- Segment: Market segment (e.g., DAM - Day Ahead Market, RTM - Real Time Market)
- Record_Date: Date of the record(YYYY-MM-DD format)
- Record_Hour: Hour of the day (0-23)
- Time_Block: Time block identifier
- Purchase_Bid_MW: Purchase bid in megawatts
- Sell_Bid_MW: Sell bid in megawatts
- MCV_MW: Market Clearing Volume in megawatts
- Final_Scheduled_Volume_MW: Final scheduled volume in megawatts
- MCP_Rs_MWh: Market Clearing Price in Rupees per MWh
- MCP_Rs_MW: Market Clearing Price in Rupees per MW

Rules:
1. Generate ONLY SELECT statements for safety
2. Use proper MySQL syntax
3. Return ONLY the SQL query, no explanations or additional text
4. For date comparisons, use DATE() function or proper date format
5. Use LIMIT when appropriate for large results
6. Common queries involve aggregations by date, hour, segment
7. When asked about "last month", "yesterday", use appropriate date functions
8. If the year is not stated, assume the current year.
9. When asked for maximum or minimum values, don't select the Segment.
10. The tables available to you are energy_bids_dam, energy_bids_gdam, energy_bids_rtm, energy_bids_tam, energy_bids_gtam.
11. If the table name is not specified, assume the query is for energy_bids_dam.
Examples:
- "Show all data" → SELECT * FROM table_name LIMIT 100;
- "Data for today" → SELECT * FROM table_name WHERE DATE(Record_Date) = CURDATE();
- "Average price by segment" → SELECT Segment, AVG(MCP_Rs_MWh) FROM table_name GROUP BY Segment;

Natural Language Query: {natural_query}

SQL Query:"""

    payload = {
        "model": LLM_CONFIG['model_name'],
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": LLM_CONFIG['temperature'],
        "max_tokens": LLM_CONFIG['max_tokens'],
        "stream": False
    }
    
    try:
        response = requests.post(
            f"{LLM_CONFIG['endpoint']}/v1/chat/completions",
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        sql_query = result["choices"][0]["message"]["content"].strip()
        
        # Clean up the SQL query
        sql_query = clean_sql(sql_query)
        
        logger.info(f"Generated SQL: {sql_query}")
        return sql_query
        
    except requests.RequestException as e:
        logger.error(f"LLM request failed: {e}")
        raise

def clean_sql(sql):
    """Clean and validate generated SQL"""
    # Remove markdown code blocks if present
    sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'```\s*', '', sql)
    
    # Remove any explanatory text before SELECT
    sql = re.sub(r'^.*?SELECT', 'SELECT', sql, flags=re.IGNORECASE | re.DOTALL)
    
    # Remove trailing explanations after semicolon
    sql = sql.split(';')[0] + ';'
    
    # Remove any remaining explanatory text after the query
    lines = sql.split('\n')
    sql_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--') and not stripped.startswith('#'):
            sql_lines.append(line)
        elif not stripped:  # Keep empty lines within the query
            sql_lines.append(line)
        else:
            break  # Stop at comments that appear after the query
    
    sql = '\n'.join(sql_lines)
    
    # Basic validation - ensure it's a SELECT query
    if not sql.strip().upper().startswith('SELECT'):
        raise ValueError("Generated query is not a SELECT statement")
    
    return sql.strip()

# Core processing function
def process_natural_query(natural_query):
    try:
        # Get database schema
        schema = db_get_schema()
        
        # Generate SQL using LLM
        sql_query = llm_generate_sql(natural_query, schema)
        
        # Execute SQL query
        results = db_execute_query(sql_query)
        
        return {
            "natural_query": natural_query,
            "generated_sql": sql_query,
            "results": results
        }
    
    except Exception as e:
        logger.error(f"Query processing failed: {e}")
        return {
            "natural_query": natural_query,
            "error": str(e),
            "success": False
        }

# Initialize database connection
db_connect()

# Flask Application
app = Flask(__name__)

@app.route('/')
def index():
    return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IEX Electricity Market Data Query</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            padding: 30px;
            text-align: center;
            color: white;
        }

        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            font-weight: 300;
        }

        .header p {
            font-size: 1.1rem;
            opacity: 0.9;
        }

        .main-content {
            padding: 40px;
        }

        .query-section {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            border: 1px solid #e9ecef;
        }

        .query-input-group {
            display: flex;
            gap: 15px;
            margin-bottom: 20px;
        }

        #queryInput {
            flex: 1;
            padding: 15px 20px;
            border: 2px solid #dee2e6;
            border-radius: 50px;
            font-size: 16px;
            outline: none;
            transition: all 0.3s ease;
        }

        #queryInput:focus {
            border-color: #4facfe;
            box-shadow: 0 0 0 3px rgba(79, 172, 254, 0.1);
        }

        .btn {
            padding: 15px 30px;
            border: none;
            border-radius: 50px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .btn-primary {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            color: white;
        }

        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 20px rgba(79, 172, 254, 0.3);
        }

        .btn-secondary {
            background: #6c757d;
            color: white;
        }

        .btn-secondary:hover {
            background: #5a6268;
            transform: translateY(-2px);
        }

        .example-queries {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }

        .example-query {
            background: white;
            border: 1px solid #dee2e6;
            border-radius: 25px;
            padding: 8px 16px;
            font-size: 14px;
            cursor: pointer;
            transition: all 0.3s ease;
        }

        .example-query:hover {
            background: #4facfe;
            color: white;
            transform: translateY(-1px);
        }

        .results-section {
            background: white;
            border-radius: 15px;
            border: 1px solid #e9ecef;
            overflow: hidden;
        }

        .results-header {
            background: #f8f9fa;
            padding: 20px;
            border-bottom: 1px solid #e9ecef;
        }

        .results-content {
            padding: 20px;
        }

        .sql-display {
            background: #2d3748;
            color: #e2e8f0;
            padding: 20px;
            border-radius: 10px;
            margin: 15px 0;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 14px;
            overflow-x: auto;
        }

        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }

        .data-table th,
        .data-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e9ecef;
        }

        .data-table th {
            background: #f8f9fa;
            font-weight: 600;
            color: #495057;
        }

        .data-table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            display: none;
            text-align: center;
            padding: 40px;
        }

        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #4facfe;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .error {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 10px;
            margin: 15px 0;
            border: 1px solid #f5c6cb;
        }

        .success {
            background: #d4edda;
            color: #155724;
            padding: 15px;
            border-radius: 10px;
            margin: 15px 0;
            border: 1px solid #c3e6cb;
        }

        .stats {
            display: flex;
            gap: 20px;
            margin: 15px 0;
        }

        .stat-item {
            background: #f8f9fa;
            padding: 10px 15px;
            border-radius: 8px;
            font-size: 14px;
        }

        .stat-value {
            font-weight: 600;
            color: #4facfe;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>IEX Electricity Market Data Query</h1>
            
        </div>

        <div class="main-content">
            <div class="query-section">
                <div class="query-input-group">
                    <input type="text" id="queryInput" placeholder="Ask about electricity market data..." />
                    <button class="btn btn-primary" onclick="executeQuery()">Query</button>
                    <button class="btn btn-secondary" onclick="showSchema()">Schema</button>
                </div>

                <div class="example-queries">
                    <div class="example-query" onclick="setQuery('Show me all data from today')">Today's data</div>
                    <div class="example-query" onclick="setQuery('What is the average market clearing price by segment?')">Avg price by segment</div>
                    <div class="example-query" onclick="setQuery('Show me the highest purchase bids this week')">Highest purchase bids</div>
                    <div class="example-query" onclick="setQuery('Which segments have the most trading volume?')">Volume by segment</div>
                    <div class="example-query" onclick="setQuery('Show me data for hour 12 across all dates')">Hour 12 data</div>
                    <div class="example-query" onclick="setQuery('What is the total scheduled volume by date for last 7 days?')">Weekly scheduled volume</div>
                </div>
            </div>

            <div class="results-section">
                <div class="results-header">
                    <h3>Results</h3>
                </div>
                <div class="results-content" id="resultsContent">
                    <p style="color: #6c757d; text-align: center; padding: 40px;">
                        Enter a query about electricity market data to see results here
                    </p>
                </div>
                <div class="loading" id="loading">
                    <div class="spinner"></div>
                    <p>Processing your query...</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        function setQuery(query) {
            document.getElementById('queryInput').value = query;
        }

        function executeQuery() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) {
                alert('Please enter a query');
                return;
            }

            showLoading(true);
            
            fetch('/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ query: query })
            })
            .then(response => response.json())
            .then(data => {
                showLoading(false);
                displayResults(data);
            })
            .catch(error => {
                showLoading(false);
                displayError('Request failed: ' + error.message);
            });
        }

        function showSchema() {
            showLoading(true);
            
            fetch('/schema')
            .then(response => response.json())
            .then(data => {
                showLoading(false);
                if (data.schema) {
                    displaySchema(data.schema);
                } else {
                    displayError('Failed to load schema: ' + (data.error || 'Unknown error'));
                }
            })
            .catch(error => {
                showLoading(false);
                displayError('Request failed: ' + error.message);
            });
        }

        function showLoading(show) {
            document.getElementById('loading').style.display = show ? 'block' : 'none';
            document.getElementById('resultsContent').style.display = show ? 'none' : 'block';
        }

        function displayResults(data) {
            const content = document.getElementById('resultsContent');
            
            if (data.error) {
                content.innerHTML = `<div class="error">Error: ${data.error}</div>`;
                return;
            }

            let html = '';
            

            if (data.results) {
                if (data.results.success) {
                    html += `
                        <div class="stats">
                            <div class="stat-item">
                                <span class="stat-value">${data.results.row_count || data.results.affected_rows || 0}</span>
                                ${data.results.row_count ? 'rows returned' : 'rows affected'}
                            </div>
                        </div>
                    `;

                    if (data.results.rows && data.results.rows.length > 0) {
                        html += '<h4>Results:</h4>';
                        html += '<table class="data-table"><thead><tr>';
                        
                        data.results.columns.forEach(col => {
                            html += `<th>${col}</th>`;
                        });
                        html += '</tr></thead><tbody>';
                        
                        data.results.rows.forEach(row => {
                            html += '<tr>';
                            row.forEach(cell => {
                                html += `<td>${cell !== null ? cell : 'NULL'}</td>`;
                            });
                            html += '</tr>';
                        });
                        html += '</tbody></table>';
                    }
                } else {
                    html += `<div class="error">SQL Error: ${data.results.error}</div>`;
                }
            }

            content.innerHTML = html;
        }

        function displaySchema(schema) {
            const content = document.getElementById('resultsContent');
            content.innerHTML = `
                <h4>Database Schema:</h4>
                <div class="sql-display">${schema.replace(/\\n/g, '<br>')}</div>
            `;
        }

        function displayError(error) {
            const content = document.getElementById('resultsContent');
            content.innerHTML = `<div class="error">${error}</div>`;
        }

        // Allow Enter key to submit query
        document.getElementById('queryInput').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                executeQuery();
            }
        });
    </script>
</body>
</html>'''

@app.route('/query', methods=['POST'])
def query():
    try:
        data = request.get_json()
        natural_query = data.get('query', '').strip()
        
        if not natural_query:
            return jsonify({'error': 'Query cannot be empty'}), 400
        
        result = process_natural_query(natural_query)
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/schema')
def schema():
    """Get database schema for reference"""
    try:
        schema = db_get_schema()
        return jsonify({'schema': schema})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.teardown_appcontext
def cleanup(error):
    """Clean up resources when app shuts down"""
    db_close()

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)