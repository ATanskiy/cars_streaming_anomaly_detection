# AI model
OPEN_AI_MODEL = "gpt-3.5-turbo"
TOOLS_CHOICE = "auto"
TEMPERATURE = 0.7
MAX_TOKENS = 1500

# Streaming jobs
STREAMING_JOBS = [
                "4_data_generator",
                "5_enriching",
                "6_alerting",
                "7_print_aggregations"
            ]

# System promt for the bot
SYSTEM_PROMT = """You are an intelligent assistant for a data platform management system. You have access to:

                    1. **Airflow**: Manage and monitor DAGs (workflows)
                    2. **Trino/Iceberg**: Query data in the data lakehouse
                    3. **Spark**: Check streaming job status
                    4. **Superset**: Access dashboards

                    Your capabilities:
                    - List, trigger, monitor, and kill Airflow DAGs
                    - Execute SQL queries on Trino
                    - Check table schemas and row counts
                    - Monitor Spark streaming jobs
                    - List available dashboards

                    Guidelines:
                    - Be proactive: If user asks "what's running?", check both DAGs and streaming jobs
                    - Confirm destructive actions: Before killing a DAG, explain what will happen
                    - Provide context: When showing query results, explain what they mean
                    - Suggest next steps: After an action, suggest related useful actions
                    - Be concise but informative
                    - Use emojis appropriately (✅ ❌ 🔄 ⚠️ 📊)

                    You can take actions autonomously - you don't need to ask permission for read operations (listing, checking status), but confirm before destructive operations (killing DAGs).

                    When a user asks something like "stop the etl dag", you should:
                    1. First check if it's actually running
                    2. Confirm with user what you found
                    3. Then proceed to kill it

                    Be conversational and helpful!"""


# Define tools/functions the AI can use
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_dags",
            "description": "List all available Airflow DAGs",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "trigger_dag",
            "description": "Trigger/run an Airflow DAG",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG to trigger"
                    }
                },
                "required": ["dag_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_dag_status",
            "description": "Check the status of a DAG's recent runs",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG to check"
                    }
                },
                "required": ["dag_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "kill_dag_run",
            "description": "Stop/kill a running DAG",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The DAG ID"
                    }
                },
                "required": ["dag_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_schemas",
            "description": "List all schemas in the Trino catalog",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List tables in a specific schema",
            "parameters": {
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "The schema name"
                    }
                },
                "required": ["schema"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "run_query",
            "description": "Execute a SQL SELECT query on Trino",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute (SELECT only)"
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_rows",
            "description": "Count rows in a table",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "The table in format: schema.table_name"
                    }
                },
                "required": ["table"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_streaming_jobs",
            "description": "Check which Spark streaming jobs are running",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_dashboards",
            "description": "List available Superset dashboards",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
]

INFO_DESCRIPTION = """
                🚗 *Cars Anomaly Detection Platform*

                Real-time monitoring system for vehicle telemetry data with AI-powered control interface.

                *What this bot can do:*

                📊 *Data Pipeline Control*
                - Start/stop data processing jobs
                - Monitor pipeline status
                - View execution history

                📈 *Analytics & Queries*
                - Run SQL queries on car data
                - Browse database schemas/tables
                - Access visualization dashboards

                🤖 *AI Assistant*
                - Natural language commands
                - "Start the ETL pipeline"
                - "How many alerts today?"
                - "Show black cars over 100 mph"

                ⚡ *Real-Time Alerts*
                - Get anomaly notifications every minute
                - Aggregated stats by car color
                - Max speed/gear/RPM tracking

                *Quick Start:*
                1. `/dags` - See available pipelines
                2. `/run 1_producer` - Start data generation
                3. `/ai` - Chat naturally with AI assistant

                Type `/help` to see all commands!
    """