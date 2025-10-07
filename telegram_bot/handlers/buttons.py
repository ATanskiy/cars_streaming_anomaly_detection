"""
Telegram bot callback handlers for interactive button menus.

This module handles all inline keyboard button callbacks for the Telegram bot,
providing interactive interfaces for managing Airflow DAGs and querying Trino databases.

Callback handlers:
    - DAG operations: dag_menu_, run_, kill_, status_ - Trigger, monitor, and stop Airflow DAGs
    - Trino browsing: schema_, table_ - Navigate database schemas and tables
    - Data operations: count_, preview_ - View row counts and data samples
    - Navigation: action_, back_to_dags - Menu navigation and actions

Key features:
    - Dynamic button menus with context-aware options
    - Real-time DAG state detection (running/queued)
    - Kill button only shown for active DAG runs
    - Chat ID tracking for DAG run notifications
    - Integrated error handling and logging

The module coordinates between airflow, trino_queries, and menus handlers to provide
a seamless interactive experience for data pipeline management.
"""

import logging, config
from telegram.ext import ContextTypes
from handlers import airflow, trino_queries, menus
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup


logger = logging.getLogger(__name__)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("dag_menu_"):
        await handle_dag_menu(query, context, data)
    elif data.startswith("kill_"):
        await handle_kill_dag(query, context, data)
    elif data.startswith("run_"):
        await handle_run_dag(query, context, data)
    elif data.startswith("status_"):
        await handle_dag_status(query, context, data)
    elif data.startswith("schema_"):
        await handle_schema_selected(query, context, data)
    elif data.startswith("table_"):
        await handle_table_selected(query, context, data)
    elif data.startswith("count_"):
        await handle_count_rows(query, context, data)
    elif data.startswith("preview_"):
        await handle_preview_data(query, context, data)
    elif data.startswith("action_"):
        await handle_action_menu(query, context, data)
    elif data == "back_to_dags":
        await menus.show_dags_with_buttons(query, context)


async def handle_dag_menu(query, context, data):
    dag_id = data.replace("dag_menu_", "")
    
    # First check if there's a running DAG
    try:
        session = airflow.get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        
        # Get more runs to debug
        response = session.get(url, params={'limit': 5, 'order_by': '-start_date'})
        response.raise_for_status()
        
        runs = response.json().get('dag_runs', [])
        has_running = False
        latest_run_id = None
        latest_state = None
        
        logger.info(f"=== Checking DAG: {dag_id} ===")
        logger.info(f"Number of runs returned: {len(runs)}")
        
        if runs:
            # Check ALL runs for running state
            for run in runs:
                run_state = run.get('state')
                run_id = run.get('dag_run_id')
                
                logger.info(f"Checking run {run_id}: state='{run_state}'")
                
                # Check if this run is active
                if run_state in ['running', 'queued']:
                    has_running = True
                    latest_run_id = run_id
                    latest_state = run_state
                    logger.info(f"✓ FOUND RUNNING RUN: {run_id}")
                    break
            
            if not has_running:
                logger.info(f"✗ NO RUNNING RUNS FOUND")
        
        keyboard = [
            [InlineKeyboardButton("▶️ Run DAG", callback_data=f"run_{dag_id}")],
            [InlineKeyboardButton("📊 Check Status", callback_data=f"status_{dag_id}")],
        ]
        
        # Add kill button if there's a running DAG
        if has_running and latest_run_id:
            # Store the full run_id in context for later use
            if not context.user_data.get('run_ids'):
                context.user_data['run_ids'] = {}
            context.user_data['run_ids'][dag_id] = latest_run_id
            
            # Use just the DAG ID in callback_data (much shorter!)
            keyboard.insert(1, [InlineKeyboardButton("🛑 Kill Running DAG", callback_data=f"kill_{dag_id}")])
            logger.info(f"✓ KILL BUTTON ADDED (run_id stored in context)")
        
        keyboard.append([InlineKeyboardButton("« Back", callback_data="back_to_dags")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        status_msg = f"\n⚠️ *Currently {latest_state.upper()}*" if has_running and latest_state else ""
        await query.edit_message_text(
            f"DAG: `{dag_id}`{status_msg}\n\nWhat would you like to do?",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"❌ ERROR checking DAG status for {dag_id}: {e}")
        logger.exception("Full traceback:")
        # Fallback to basic menu
        keyboard = [
            [InlineKeyboardButton("▶️ Run DAG", callback_data=f"run_{dag_id}")],
            [InlineKeyboardButton("📊 Check Status", callback_data=f"status_{dag_id}")],
            [InlineKeyboardButton("« Back", callback_data="back_to_dags")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"DAG: `{dag_id}`\n\nWhat would you like to do?",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def handle_kill_dag(query, context, data):
    parts = data.replace("kill_", "").split("_", 1)
    if len(parts) != 2:
        await query.edit_message_text("❌ Invalid kill command format")
        return
    
    dag_id, run_id = parts
    await query.edit_message_text(f"🛑 Killing DAG run `{run_id}`...", parse_mode='Markdown')
    
    try:
        session = airflow.get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        
        payload = {"state": "failed"}
        
        response = session.patch(url, json=payload)
        response.raise_for_status()
        
        await query.edit_message_text(
            f"🛑 DAG run killed successfully!\n"
            f"DAG: `{dag_id}`\n"
            f"Run ID: `{run_id}`",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error killing DAG: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_run_dag(query, context, data):
    dag_id = data.replace("run_", "")
    chat_id = query.message.chat_id
    
    # DEBUG: Log the chat_id
    logger.info(f"★★★ Chat ID for notifications: {chat_id}")
    
    await query.edit_message_text(f"Triggering DAG `{dag_id}`...", parse_mode='Markdown')
    
    try:
        session = airflow.get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        
        payload = {
            "conf": {
                "telegram_chat_id": str(chat_id)
            }
        }
        
        # DEBUG: Log the payload
        logger.info(f"★★★ Payload being sent: {payload}")
        
        response = session.post(url, json=payload)
        response.raise_for_status()
        
        run_data = response.json()
        run_id = run_data.get('dag_run_id', 'unknown')
        
        await query.edit_message_text(
            f"✅ DAG `{dag_id}` triggered successfully!\nRun ID: `{run_id}`\n\n"
            f"Chat ID: `{chat_id}` - you should receive updates here.",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error triggering DAG: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_dag_status(query, context, data):
    dag_id = data.replace("status_", "")
    await query.edit_message_text(f"Checking status of `{dag_id}`...", parse_mode='Markdown')
    
    try:
        session = airflow.get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        response = session.get(url, params={'limit': 5, 'order_by': '-execution_date'})
        response.raise_for_status()
        
        runs = response.json().get('dag_runs', [])
        
        if not runs:
            await query.edit_message_text(f"No runs found for DAG `{dag_id}`.")
            return
        
        message = f"📊 *Status for DAG:* `{dag_id}`\n\n"
        for run in runs:
            state = run['state']
            emoji = {'success': '✅', 'running': '🔄', 'failed': '❌', 'queued': '⏳'}.get(state, '❓')
            exec_date = run['execution_date'][:19]
            message += f"{emoji} {state.upper()}\n   {exec_date}\n\n"
        
        await query.edit_message_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error checking DAG status: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_kill_dag(query, context, data):
    dag_id = data.replace("kill_", "")
    await query.edit_message_text(f"Stopping DAG `{dag_id}`...", parse_mode='Markdown')
    
    try:
        session = airflow.get_airflow_session()
        
        # Get all running DAG runs
        runs_url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        response = session.get(runs_url, params={'state': 'running'})
        response.raise_for_status()
        
        runs = response.json().get('dag_runs', [])
        
        if not runs:
            await query.edit_message_text(f"No running instances found for DAG `{dag_id}`.")
            return
        
        killed_count = 0
        
        # Stop each running DAG run
        for run in runs:
            run_id = run['dag_run_id']
            update_url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
            payload = {"state": "failed"}
            
            update_response = session.patch(update_url, json=payload)
            update_response.raise_for_status()
            killed_count += 1
        
        # Pause the DAG
        pause_url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}"
        pause_payload = {"is_paused": True}
        session.patch(pause_url, json=pause_payload)
        
        await query.edit_message_text(
            f"🛑 Stopped {killed_count} running instance(s) of `{dag_id}`\n"
            f"DAG has been paused.\n\n"
            f"Use 'Run DAG' button to restart when ready.",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error killing DAG: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_schema_selected(query, context, data):
    SCHEMA_NAME = data.replace("schema_", "")
    await query.edit_message_text(f"Loading tables in `{SCHEMA_NAME}`...", parse_mode='Markdown')
    
    try:
        conn = trino_queries.get_trino_connection()
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN {config.TRINO_CATALOG}.{SCHEMA_NAME}")
        tables = cursor.fetchall()
        
        if not tables:
            await query.edit_message_text(f"No tables found in schema `{SCHEMA_NAME}`.")
            return
        
        keyboard = []
        for table in tables[:20]:
            table_name = table[0]
            keyboard.append([
                InlineKeyboardButton(f"📊 {table_name}", callback_data=f"table_{SCHEMA_NAME}.{table_name}")
            ])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="action_schemas")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Tables in `{SCHEMA_NAME}`:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_table_selected(query, context, data):
    table_full = data.replace("table_", "")
    keyboard = [
        [InlineKeyboardButton("🔢 Count Rows", callback_data=f"count_{table_full}")],
        [InlineKeyboardButton("👁️ Preview Data", callback_data=f"preview_{table_full}")],
        [InlineKeyboardButton("« Back", callback_data="action_schemas")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Table: `{table_full}`\n\nWhat would you like to do?",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def handle_count_rows(query, context, data):
    table = data.replace("count_", "")
    await query.edit_message_text(f"Counting rows in `{table}`...", parse_mode='Markdown')
    
    try:
        conn = trino_queries.get_trino_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {config.TRINO_CATALOG}.{table}")
        count = cursor.fetchone()[0]
        await query.edit_message_text(
            f"📊 Table `{table}` has *{count:,}* rows",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error counting rows: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_preview_data(query, context, data):
    table = data.replace("preview_", "")
    await query.edit_message_text(f"Fetching preview of `{table}`...", parse_mode='Markdown')
    
    try:
        conn = trino_queries.get_trino_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {config.TRINO_CATALOG}.{table} LIMIT 5")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        if not rows:
            await query.edit_message_text("No data found.")
            return
        
        message = f"📊 *Preview of {table}:*\n\n```\n"
        message += " | ".join(columns) + "\n"
        message += "-" * 50 + "\n"
        for row in rows:
            message += " | ".join(str(val)[:20] for val in row) + "\n"
        message += "```"
        
        await query.edit_message_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error previewing data: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


async def handle_action_menu(query, context, data):
    if data == "action_schemas":
        await menus.show_schemas_with_buttons(query, context)
    elif data == "action_browse_tables":
        await query.edit_message_text("Please use /tables <SCHEMA_NAME> or select a schema from the menu.")
    elif data == "action_custom_query":
        await query.edit_message_text("Use: /query SELECT * FROM schema.table LIMIT 10")