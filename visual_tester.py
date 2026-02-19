#!/usr/bin/env python3
"""
Visual Testing Interface for PySpark Tools
Interactive web UI for data engineers to test SQL conversion
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from flask import Flask, render_template, request, jsonify
from pyspark_tools.sql_converter import SQLToPySparkConverter
from pyspark_tools.advanced_optimizer import AdvancedOptimizer
import traceback

app = Flask(__name__)
converter = SQLToPySparkConverter()
optimizer = AdvancedOptimizer()

SAMPLE_QUERIES = {
    "simple": """SELECT customer_id, SUM(amount) as total
FROM orders
WHERE status = 'completed'
GROUP BY customer_id
HAVING total > 1000""",
    "join": """SELECT c.name, COUNT(o.id) as order_count
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE c.active = true
GROUP BY c.name""",
    "window": """SELECT 
    product_id,
    sale_date,
    amount,
    SUM(amount) OVER (PARTITION BY product_id ORDER BY sale_date) as running_total
FROM sales""",
    "complex": """WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        customer_id,
        SUM(amount) as total
    FROM orders
    GROUP BY 1, 2
)
SELECT 
    month,
    COUNT(DISTINCT customer_id) as customers,
    AVG(total) as avg_order
FROM monthly_sales
GROUP BY month
ORDER BY month DESC"""
}

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <title>PySpark Tools - Visual Tester</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 10px; }
        .subtitle { color: #666; margin-bottom: 30px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .panel { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .panel h2 { color: #333; margin-bottom: 15px; font-size: 18px; }
        textarea { 
            width: 100%; 
            height: 300px; 
            font-family: 'Monaco', 'Courier New', monospace;
            font-size: 13px;
            padding: 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            resize: vertical;
        }
        .output { 
            background: #f8f9fa;
            border: 1px solid #e1e4e8;
            border-radius: 4px;
            padding: 12px;
            font-family: 'Monaco', 'Courier New', monospace;
            font-size: 13px;
            white-space: pre-wrap;
            max-height: 500px;
            overflow-y: auto;
        }
        .buttons { display: flex; gap: 10px; margin: 15px 0; }
        button {
            padding: 10px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
        }
        .btn-primary { background: #0366d6; color: white; }
        .btn-primary:hover { background: #0256c7; }
        .btn-secondary { background: #6c757d; color: white; }
        .btn-secondary:hover { background: #5a6268; }
        .samples { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 15px; }
        .sample-btn {
            padding: 6px 12px;
            background: #f1f3f5;
            border: 1px solid #ddd;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
        }
        .sample-btn:hover { background: #e9ecef; }
        .stats { 
            display: grid; 
            grid-template-columns: repeat(4, 1fr); 
            gap: 10px; 
            margin-top: 15px;
        }
        .stat {
            background: #f8f9fa;
            padding: 12px;
            border-radius: 4px;
            text-align: center;
        }
        .stat-value { font-size: 24px; font-weight: bold; color: #0366d6; }
        .stat-label { font-size: 12px; color: #666; margin-top: 4px; }
        .error { color: #d73a49; background: #ffeef0; padding: 12px; border-radius: 4px; }
        .success { color: #28a745; background: #dcffe4; padding: 12px; border-radius: 4px; }
        .optimization { 
            background: #fff3cd; 
            border-left: 4px solid #ffc107;
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
        }
        .opt-title { font-weight: bold; color: #856404; margin-bottom: 5px; }
        .opt-desc { font-size: 13px; color: #666; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 PySpark Tools - Visual Tester</h1>
        <p class="subtitle">Test SQL to PySpark conversion with real-time optimization suggestions</p>
        
        <div class="grid">
            <div class="panel">
                <h2>📝 SQL Input</h2>
                <div class="samples">
                    <button class="sample-btn" onclick="loadSample('simple')">Simple Query</button>
                    <button class="sample-btn" onclick="loadSample('join')">Join Query</button>
                    <button class="sample-btn" onclick="loadSample('window')">Window Function</button>
                    <button class="sample-btn" onclick="loadSample('complex')">Complex CTE</button>
                </div>
                <textarea id="sqlInput" placeholder="Enter your SQL query here...">{{ sample_query }}</textarea>
                <div class="buttons">
                    <button class="btn-primary" onclick="convert()">Convert to PySpark</button>
                    <button class="btn-secondary" onclick="clearAll()">Clear</button>
                </div>
            </div>
            
            <div class="panel">
                <h2>⚡ PySpark Output</h2>
                <div id="output" class="output">Click "Convert to PySpark" to see results...</div>
                <div class="buttons">
                    <button class="btn-secondary" onclick="copyOutput()">Copy Code</button>
                </div>
            </div>
        </div>
        
        <div class="panel" style="margin-top: 20px;">
            <h2>🎯 Optimization Suggestions</h2>
            <div id="optimizations">Run conversion to see optimization suggestions...</div>
        </div>
        
        <div class="stats">
            <div class="stat">
                <div class="stat-value" id="conversionTime">-</div>
                <div class="stat-label">Conversion Time (ms)</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="linesCount">-</div>
                <div class="stat-label">Lines Generated</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="optimizationCount">-</div>
                <div class="stat-label">Optimizations Found</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="complexityScore">-</div>
                <div class="stat-label">Complexity Score</div>
            </div>
        </div>
    </div>
    
    <script>
        const samples = {{ samples | tojson }};
        
        function loadSample(type) {
            document.getElementById('sqlInput').value = samples[type];
        }
        
        async function convert() {
            const sql = document.getElementById('sqlInput').value;
            if (!sql.trim()) {
                alert('Please enter a SQL query');
                return;
            }
            
            const startTime = performance.now();
            
            try {
                const response = await fetch('/convert', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({sql: sql})
                });
                
                const data = await response.json();
                const endTime = performance.now();
                
                if (data.error) {
                    document.getElementById('output').innerHTML = 
                        `<div class="error">❌ Error: ${data.error}</div>`;
                    return;
                }
                
                document.getElementById('output').textContent = data.pyspark_code;
                
                // Display optimizations
                let optHtml = '';
                if (data.optimizations && data.optimizations.length > 0) {
                    data.optimizations.forEach(opt => {
                        optHtml += `
                            <div class="optimization">
                                <div class="opt-title">${opt.type}: ${opt.title}</div>
                                <div class="opt-desc">${opt.description}</div>
                            </div>
                        `;
                    });
                } else {
                    optHtml = '<div class="success">✅ No optimizations needed - code looks good!</div>';
                }
                document.getElementById('optimizations').innerHTML = optHtml;
                
                // Update stats
                document.getElementById('conversionTime').textContent = 
                    Math.round(endTime - startTime);
                document.getElementById('linesCount').textContent = 
                    data.pyspark_code.split('\\n').length;
                document.getElementById('optimizationCount').textContent = 
                    data.optimizations ? data.optimizations.length : 0;
                document.getElementById('complexityScore').textContent = 
                    data.complexity || 'Low';
                
            } catch (error) {
                document.getElementById('output').innerHTML = 
                    `<div class="error">❌ Error: ${error.message}</div>`;
            }
        }
        
        function copyOutput() {
            const output = document.getElementById('output').textContent;
            navigator.clipboard.writeText(output);
            alert('Code copied to clipboard!');
        }
        
        function clearAll() {
            document.getElementById('sqlInput').value = '';
            document.getElementById('output').textContent = 'Click "Convert to PySpark" to see results...';
            document.getElementById('optimizations').textContent = 'Run conversion to see optimization suggestions...';
            document.getElementById('conversionTime').textContent = '-';
            document.getElementById('linesCount').textContent = '-';
            document.getElementById('optimizationCount').textContent = '-';
            document.getElementById('complexityScore').textContent = '-';
        }
    </script>
</body>
</html>"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, 
                                 sample_query=SAMPLE_QUERIES['simple'],
                                 samples=SAMPLE_QUERIES)

@app.route('/convert', methods=['POST'])
def convert():
    try:
        data = request.json
        sql = data.get('sql', '')
        
        # Convert SQL to PySpark
        result = converter.convert_sql_to_pyspark(sql)
        pyspark_code = result.get('pyspark_code', '')
        
        # Get optimizations
        opt_result = optimizer.analyze_and_optimize(pyspark_code)
        optimizations = opt_result.get('optimizations', [])
        
        # Calculate complexity
        complexity = 'Low'
        if len(sql.split()) > 50:
            complexity = 'Medium'
        if 'JOIN' in sql.upper() and 'GROUP BY' in sql.upper():
            complexity = 'High'
        
        return jsonify({
            'pyspark_code': pyspark_code,
            'optimizations': optimizations[:5],  # Top 5
            'complexity': complexity
        })
        
    except Exception as e:
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 400

def render_template_string(template, **context):
    """Simple template renderer"""
    import json
    result = template
    for key, value in context.items():
        if key == 'samples':
            result = result.replace('{{ samples | tojson }}', json.dumps(value))
        else:
            result = result.replace('{{ ' + key + ' }}', str(value))
    return result

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 PySpark Tools Visual Tester")
    print("="*60)
    print("\n📍 Open in browser: http://localhost:5000")
    print("🔧 For data engineers to test SQL conversion visually")
    print("\nPress Ctrl+C to stop\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
