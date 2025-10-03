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


async def handle_run_dag(query, context, data):
    dag_id = data.replace("run_", "")
    await query.edit_message_text(f"Triggering DAG `{dag_id}`...", parse_mode='Markdown')
    
    try:
        session = airflow.get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        
        # Simplified payload - let Airflow set the execution date
        payload = {}
        
        response = session.post(url, json=payload)
        response.raise_for_status()
        
        run_data = response.json()
        run_id = run_data.get('dag_run_id', 'unknown')
        
        await query.edit_message_text(
            f"✅ DAG `{dag_id}` triggered successfully!\nRun ID: `{run_id}`",
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