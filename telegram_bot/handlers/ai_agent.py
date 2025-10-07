"""
Autonomous AI Agent for data platform management via Telegram bot.

This module implements an intelligent conversational agent powered by OpenAI's GPT
with function calling capabilities. The AI can autonomously control and monitor
the entire data platform through natural language conversation.

Core capabilities:
    - Airflow: List, trigger, monitor, and kill DAGs
    - Trino: Execute queries, browse schemas/tables, count rows
    - Spark: Monitor streaming job status
    - Superset: List and access dashboards

Key features:
    - Function calling: AI autonomously invokes platform operations
    - Conversational context: Maintains chat history for coherent multi-turn conversations
    - Proactive assistance: Suggests next steps and confirms destructive actions
    - Smart history management: Cleans conversation history to prevent API errors
    - Typing indicators and progress feedback for better UX

Functions:
    execute_function() - Executes requested platform operations and returns results
    get_system_prompt() - Defines AI personality and operational guidelines
    ai_chat() - Main conversation handler with function calling loop
    start_ai_mode() - Activates AI assistant mode
    handle_ai_button() - Handles AI mode activation from inline buttons

AI behavior:
    - Read operations execute automatically (listing, status checks)
    - Destructive operations confirm before execution (killing DAGs)
    - Provides context and explanations with results
    - Uses emojis for visual feedback (✅ ❌ 🔄 ⚠️ 📊)
    - Suggests related actions after completing tasks

The agent integrates with all platform handlers (airflow, trino_queries, spark, 
superset) to provide a unified natural language interface for data platform management.
"""

import logging, json, openai, config, constants
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from handlers import airflow, trino_queries, spark, superset

logger = logging.getLogger(__name__)

openai.api_key = config.OPENAI_API_KEY

async def execute_function(function_name: str, arguments: dict, chat_id: int):
    """Execute the requested function and return results."""
    try:
        if function_name == "list_dags":
            session = airflow.get_airflow_session()
            url = f"{config.AIRFLOW_API_URL}/api/v1/dags"
            response = session.get(url, params={'limit': 100})
            response.raise_for_status()
            dags = response.json().get('dags', [])
            return {
                "dags": [{"dag_id": d['dag_id'], "is_paused": d['is_paused']} for d in dags]
            }
        
        elif function_name == "trigger_dag":
            dag_id = arguments['dag_id']
            session = airflow.get_airflow_session()
            url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
            
            payload = {
                "conf": {
                    "telegram_chat_id": str(chat_id)
                }
            }
            
            response = session.post(url, json=payload)
            response.raise_for_status()
            run_data = response.json()
            
            return {
                "success": True,
                "dag_id": dag_id,
                "run_id": run_data.get('dag_run_id', 'unknown')
            }
        
        elif function_name == "check_dag_status":
            dag_id = arguments['dag_id']
            session = airflow.get_airflow_session()
            url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
            
            response = session.get(url, params={'limit': 5, 'order_by': '-execution_date'})
            response.raise_for_status()
            runs = response.json().get('dag_runs', [])
            
            return {
                "dag_id": dag_id,
                "runs": [{"state": r['state'], "execution_date": r['execution_date'][:19]} for r in runs]
            }
        
        elif function_name == "kill_dag_run":
            dag_id = arguments['dag_id']
            session = airflow.get_airflow_session()
            
            # Get running DAG runs
            runs_url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
            response = session.get(runs_url, params={'state': 'running'})
            response.raise_for_status()
            
            runs = response.json().get('dag_runs', [])
            
            if not runs:
                return {"success": False, "message": "No running instances found"}
            
            killed_count = 0
            for run in runs:
                run_id = run['dag_run_id']
                update_url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
                payload = {"state": "failed"}
                
                update_response = session.patch(update_url, json=payload)
                update_response.raise_for_status()
                killed_count += 1
            
            return {
                "success": True,
                "dag_id": dag_id,
                "killed_count": killed_count
            }
        
        elif function_name == "list_schemas":
            conn = trino_queries.get_trino_connection()
            cursor = conn.cursor()
            cursor.execute(f"SHOW SCHEMAS IN {config.TRINO_CATALOG}")
            schemas = [s[0] for s in cursor.fetchall() if s[0] != 'information_schema']
            return {"schemas": schemas}
        
        elif function_name == "list_tables":
            schema = arguments['schema']
            conn = trino_queries.get_trino_connection()
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES IN {config.TRINO_CATALOG}.{schema}")
            tables = [t[0] for t in cursor.fetchall()]
            return {"schema": schema, "tables": tables}
        
        elif function_name == "run_query":
            query = arguments['query']
            
            if not query.strip().upper().startswith('SELECT'):
                return {"error": "Only SELECT queries are allowed"}
            
            conn = trino_queries.get_trino_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Limit results for AI context
            result_rows = [list(row) for row in rows[:50]]
            
            return {
                "columns": columns,
                "row_count": len(rows),
                "rows": result_rows,
                "truncated": len(rows) > 50
            }
        
        elif function_name == "count_rows":
            table = arguments['table']
            conn = trino_queries.get_trino_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {config.TRINO_CATALOG}.{table}")
            count = cursor.fetchone()[0]
            return {"table": table, "count": count}
        
        elif function_name == "check_streaming_jobs":
            import subprocess
            result = subprocess.run(
                ["docker", "exec", "spark", "ps", "aux"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            lines = result.stdout.split('\n')
            streaming_jobs = set()
            
            for line in lines:
                for job in constants.STREAMING_JOBS:
                    if job in line:
                        streaming_jobs.add(job)
                        break
            
            return {"running_jobs": list(streaming_jobs)}
        
        elif function_name == "list_dashboards":
            token = superset.get_superset_token()
            if not token:
                return {"error": "Failed to authenticate with Superset"}
            
            import requests
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(
                f"{config.SUPERSET_URL}/api/v1/dashboard/",
                headers=headers,
                timeout=5
            )
            response.raise_for_status()
            
            dashboards = response.json().get('result', [])
            return {
                "dashboards": [
                    {
                        "title": d.get('dashboard_title', 'Untitled'),
                        "id": d.get('id'),
                        "url": f"{config.SUPERSET_PUBLIC_URL}/superset/dashboard/{d.get('id')}/"
                    }
                    for d in dashboards[:10]
                ]
            }
        
        else:
            return {"error": f"Unknown function: {function_name}"}
    
    except Exception as e:
        logger.error(f"Error executing {function_name}: {e}")
        return {"error": str(e)}


def get_system_prompt():
    """System prompt for the AI agent."""
    return constants.SYSTEM_PROMT


async def ai_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main AI chat interface - handles all conversation and actions."""
    
    # Initialize AI mode if not set
    if 'ai_mode' not in context.user_data:
        context.user_data['ai_mode'] = False
    
    # If not in AI mode, this shouldn't be called
    if not context.user_data['ai_mode']:
        return
    
    user_message = update.message.text
    chat_id = update.message.chat_id
    
    # Commands to exit AI mode
    if user_message.lower() in ['/exit', '/stop', '/quit', 'exit', 'quit', 'stop']:
        context.user_data['ai_mode'] = False
        context.user_data['ai_history'] = []
        await update.message.reply_text(
            "👋 Exited AI mode.\n\n"
            "Your buttons are working normally again!\n"
            "Click 🤖 AI Assistant to return."
        )
        return
    
    # Show typing indicator
    await update.message.chat.send_action("typing")
    
    try:
        # Initialize conversation history
        if 'ai_history' not in context.user_data:
            context.user_data['ai_history'] = []
        
        # Add user message
        context.user_data['ai_history'].append({
            "role": "user",
            "content": user_message
        })
        
        # FIXED: Clean history to prevent tool message errors
        # Keep only user and assistant messages (no orphaned tool messages)
        clean_history = []
        i = 0
        while i < len(context.user_data['ai_history']):
            msg = context.user_data['ai_history'][i]
            
            if msg.get('role') in ['user', 'system']:
                clean_history.append(msg)
                i += 1
            elif msg.get('role') == 'assistant':
                # If assistant has tool_calls, include the following tool messages
                clean_history.append(msg)
                i += 1
                
                # Include tool responses that follow
                while i < len(context.user_data['ai_history']) and context.user_data['ai_history'][i].get('role') == 'tool':
                    clean_history.append(context.user_data['ai_history'][i])
                    i += 1
            else:
                # Skip orphaned tool messages
                i += 1
        
        # Keep last 15 messages
        if len(clean_history) > 15:
            clean_history = clean_history[-15:]
        
        context.user_data['ai_history'] = clean_history
        
        # Call OpenAI with function calling
        response = openai.ChatCompletion.create(
            model=constants.OPEN_AI_MODEL,
            messages=[
                {"role": "system", "content": get_system_prompt()},
                *context.user_data['ai_history']
            ],
            tools=constants.TOOLS,
            tool_choice="auto",
            temperature=constants.TEMPERATURE,
            max_tokens=constants.MAX_TOKENS
        )
        
        message = response.choices[0].message
        
        # Check if AI wants to call functions
        if message.get('tool_calls'):
            # Execute all requested function calls
            function_results = []
            
            for tool_call in message.tool_calls:
                function_name = tool_call.function.name
                function_args = json.loads(tool_call.function.arguments)
                
                logger.info(f"AI calling function: {function_name} with args: {function_args}")
                
                # Show what AI is doing
                await update.message.reply_text(f"⚙️ {function_name}...")
                
                # Execute function
                result = await execute_function(function_name, function_args, chat_id)
                
                function_results.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": function_name,
                    "content": json.dumps(result)
                })
            
            # Add AI's function call to history
            context.user_data['ai_history'].append({
                "role": "assistant",
                "content": message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments
                        }
                    }
                    for tc in message.tool_calls
                ]
            })
            
            # Add function results
            context.user_data['ai_history'].extend(function_results)
            
            # Get AI's response after function execution
            second_response = openai.ChatCompletion.create(
                model=constants.OPEN_AI_MODEL,
                messages=[
                    {"role": "system", "content": get_system_prompt()},
                    *context.user_data['ai_history']
                ],
                temperature=constants.TEMPERATURE,
                max_tokens=constants.MAX_TOKENS
            )
            
            final_message = second_response.choices[0].message.content
            
        else:
            # No function calls, just regular response
            final_message = message.content
        
        # Add AI response to history
        context.user_data['ai_history'].append({
            "role": "assistant",
            "content": final_message
        })
        
        # Send response to user
        await update.message.reply_text(final_message)
        
    except Exception as e:
        logger.error(f"Error in AI chat: {e}")
        await update.message.reply_text(
            f"❌ Error: {str(e)}\n\n"
            f"Type 'exit' to leave AI mode."
        )


async def start_ai_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activate AI assistant mode."""
    context.user_data['ai_mode'] = True
    context.user_data['ai_history'] = []
    
    await update.message.reply_text(
        "🤖 *AI Assistant Activated*\n\n"
        "I can help you with:\n"
        "• Managing Airflow DAGs\n"
        "• Querying Trino data\n"
        "• Checking streaming jobs\n"
        "• Viewing dashboards\n\n"
        "Just chat naturally! Examples:\n"
        "• 'What DAGs are running?'\n"
        "• 'Run the ETL pipeline'\n"
        "• 'How many rows in users table?'\n"
        "• 'Stop all running tasks'\n\n"
        "Type 'exit' to leave AI mode.",
        parse_mode='Markdown'
    )


async def handle_ai_button(query, context):
    """Handle the AI button press from menu."""
    context.user_data['ai_mode'] = True
    context.user_data['ai_history'] = []
    
    await query.edit_message_text(
        "🤖 *AI Assistant Activated*\n\n"
        "I can help you manage your data platform!\n\n"
        "Try asking:\n"
        "• 'Show me all DAGs'\n"
        "• 'Start the data_ingestion DAG'\n"
        "• 'What's running right now?'\n"
        "• 'Kill the failing DAG'\n"
        "• 'Count rows in sales.orders'\n\n"
        "Type 'exit' to return to menu.",
        parse_mode='Markdown'
    )