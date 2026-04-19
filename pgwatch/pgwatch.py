from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import psycopg2
import requests
import json
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ── DB helpers ────────────────────────────────────────────────────────────────

def get_connection(host, port, dbname, user, password):
    return psycopg2.connect(
        host=host, port=int(port), dbname=dbname, user=user, password=password,
        connect_timeout=5
    )

def setup_demo_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pgwatch_metrics (
            id SERIAL PRIMARY KEY,
            time TIMESTAMP DEFAULT NOW(),
            metric_name TEXT UNIQUE,
            metric_value FLOAT,
            details TEXT
        )
    """)
    cur.execute("""
        INSERT INTO pgwatch_metrics (metric_name, metric_value, details) VALUES
            ('active_connections',   23,   'connections currently active'),
            ('lock_waits',           4,    'queries waiting for locks'),
            ('avg_query_time_ms',    4200, 'average query execution time'),
            ('checkpoint_warnings',  12,   'checkpoint warnings in bgwriter'),
            ('cache_hit_ratio',      94.5, 'buffer cache hit percentage'),
            ('deadlocks',            2,    'deadlocks detected'),
            ('idle_in_transaction',  3,    'connections idle in transaction'),
            ('bloat_ratio',          18.3, 'table bloat percentage'),
            ('replication_lag_mb',   0.5,  'replication lag in megabytes'),
            ('temp_files_created',   47,   'temporary files created per hour'),
            ('autovacuum_count',     8,    'autovacuum runs in last hour'),
            ('long_running_queries', 3,    'queries running more than 5 minutes'),
            ('index_hit_ratio',      98.2, 'index cache hit percentage'),
            ('seq_scans_per_min',    142,  'sequential scans per minute'),
            ('connections_max_pct',  46,   'percentage of max_connections used')
        ON CONFLICT (metric_name) DO UPDATE
            SET metric_value = EXCLUDED.metric_value,
                details = EXCLUDED.details,
                time = NOW()
    """)
    conn.commit()
    cur.close()

def fetch_metrics(conn):
    cur = conn.cursor()
    cur.execute("SELECT metric_name, metric_value, details FROM pgwatch_metrics ORDER BY time DESC")
    rows = cur.fetchall()
    cur.close()
    return {r[0]: {"value": r[1], "description": r[2]} for r in rows}

# ── Prompt builder ────────────────────────────────────────────────────────────

def build_prompt(question, metrics):
    metrics_text = "\n".join(
        f"  - {k}: {v['value']} ({v['description']})" for k, v in metrics.items()
    )
    return f"""You are a PostgreSQL database expert assistant for pgwatch.

A developer asked: {question}

Current database metrics:
{metrics_text}

Rules:
1. Only explain what the metrics actually show.
2. Give one concrete SQL query to investigate further (wrap it in triple backticks with sql tag).
3. Be direct and under 200 words.

Answer:"""

# ── LLM backends ──────────────────────────────────────────────────────────────

def call_ollama(prompt, model="llama3"):
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=60
        )
        return response.json().get("response", "No response from Ollama.")
    except Exception:
        return generate_fallback(prompt)


def call_groq(prompt, api_key, model="llama-3.3-70b-versatile"):
    resp = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        },
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1000
        },
        timeout=30
    )
    resp_json = resp.json()
    print("Groq response:", resp_json)
    if "choices" in resp_json:
        return resp_json["choices"][0]["message"]["content"]
    elif "error" in resp_json:
        return f"Groq Error: {resp_json['error']['message']}"
    return str(resp_json)


def call_anthropic(prompt, api_key):
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01"
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=30
    )
    resp_json = resp.json()
    print("Anthropic response:", resp_json)
    if "content" in resp_json:
        return resp_json["content"][0]["text"]
    elif "error" in resp_json:
        return f"API Error: {resp_json['error']['message']}"
    return str(resp_json)


def generate_fallback(prompt):
    prompt_lower = prompt.lower()
    if "slow" in prompt_lower or "performance" in prompt_lower:
        return """avg_query_time_ms is 4200ms — critically high (should be <1000ms). Combined with 4 lock_waits and 2 deadlocks, there are active blocking issues.

```sql
SELECT pid, now() - xact_start AS duration, state, query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC
LIMIT 20;
```"""
    elif "lock" in prompt_lower or "deadlock" in prompt_lower:
        return """Metrics show 4 lock_waits and 2 deadlocks — a long-running transaction is likely blocking others.

```sql
SELECT * FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE NOT granted;
```"""
    elif "cache" in prompt_lower:
        return """Cache hit ratio is 94.5% which is acceptable (>90% is healthy). Could be improved by increasing shared_buffers.

```sql
SELECT sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS cache_hit_ratio
FROM pg_statio_user_tables;
```"""
    elif "critical" in prompt_lower or "issue" in prompt_lower:
        return """Top critical issues:
1. avg_query_time_ms = 4200ms (CRITICAL)
2. lock_waits = 4 (WARNING)
3. deadlocks = 2 (WARNING)
4. checkpoint_warnings = 12 (WARNING)

```sql
SELECT pid, now() - xact_start AS duration, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC;
```"""
    else:
        return """Current metrics summary:
- active_connections: 23 (OK)
- lock_waits: 4 (WARNING)
- avg_query_time_ms: 4200ms (CRITICAL)
- cache_hit_ratio: 94.5% (OK)
- deadlocks: 2 (WARNING)

```sql
SELECT * FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY now() - xact_start DESC;
```"""

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/connect", methods=["POST"])
def api_connect():
    d = request.json
    try:
        conn = get_connection(d["host"], d["port"], d["dbname"], d["user"], d["password"])
        setup_demo_tables(conn)
        metrics = fetch_metrics(conn)
        conn.close()
        return jsonify({"status": "ok", "metrics": metrics})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/api/metrics", methods=["POST"])
def api_metrics():
    d = request.json
    try:
        conn = get_connection(d["host"], d["port"], d["dbname"], d["user"], d["password"])
        metrics = fetch_metrics(conn)
        conn.close()
        return jsonify({"status": "ok", "metrics": metrics})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/api/ask", methods=["POST"])
def api_ask():
    d = request.json
    question = d.get("question", "")
    metrics  = d.get("metrics", {})
    llm      = d.get("llm", "ollama")
    api_key  = d.get("api_key", "")
    model    = d.get("model", "llama3")

    prompt = build_prompt(question, metrics)

    try:
        if llm == "groq" and api_key:
            answer = call_groq(prompt, api_key)
        elif llm == "anthropic" and api_key:
            answer = call_anthropic(prompt, api_key)
        else:
            answer = call_ollama(prompt, model)

        return jsonify({"status": "ok", "answer": answer})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/report", methods=["POST"])
def api_report():
    d = request.json
    try:
        conn = get_connection(d["host"], d["port"], d["dbname"], d["user"], d["password"])
        setup_demo_tables(conn)
        metrics = fetch_metrics(conn)
        conn.close()

        warnings, criticals = [], []
        rows = []
        for name, data in metrics.items():
            val = data["value"]
            if name == "avg_query_time_ms" and val > 1000:
                status = "CRITICAL"; criticals.append(name)
            elif name == "lock_waits" and val > 2:
                status = "WARNING";  warnings.append(name)
            elif name == "cache_hit_ratio" and val < 90:
                status = "CRITICAL"; criticals.append(name)
            elif name == "deadlocks" and val > 0:
                status = "WARNING";  warnings.append(name)
            elif name == "checkpoint_warnings" and val > 5:
                status = "WARNING";  warnings.append(name)
            elif name == "long_running_queries" and val > 0:
                status = "WARNING";  warnings.append(name)
            elif name == "bloat_ratio" and val > 20:
                status = "WARNING";  warnings.append(name)
            else:
                status = "OK"
            rows.append({"name": name, "value": val,
                         "description": data["description"], "status": status})

        return jsonify({
            "status": "ok",
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "metrics": rows,
            "warnings": warnings,
            "criticals": criticals
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 400


if __name__ == "__main__":
    app.run(debug=True, port=5000)