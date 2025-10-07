import logging, config
from telegram.ext import ContextTypes
from handlers import airflow, trino_queries, spark, superset, ai_agent
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, \
      InlineKeyboardMarkup


logger = logging.getLogger(__name__)


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ["📋 List DAGs", "🕐 Recent Runs"],
        ["📂 List Schemas", "📊 Query Data"],
        ["🔥 Streaming Jobs", "📈 Dashboards"],
        ["🤖 AI Assistant", "❓ Help"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    user = update.effective_user
    await update.message.reply_text(
        f"Hi {user.first_name}! Welcome to Cars Anomaly Detection Bot.\n\n"
        "Choose an option below or use commands:",
        reply_markup=reply_markup
    )


async def handle_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "🤖 AI Assistant":
        # If already in AI mode, just acknowledge
        if context.user_data.get('ai_mode', False):
            await update.message.reply_text(
                "🤖 You're already in AI mode!\n\n"
                "Just type your questions naturally.\n"
                "Click any button to exit AI mode and use normal controls."
            )
        else:
            await ai_agent.start_ai_mode(update, context)
    elif text == "📋 List DAGs":
        await show_dags_with_buttons(update, context)
    elif text == "🕐 Recent Runs":
        await airflow.recent_runs(update, context)
    elif text == "📂 List Schemas":
        await show_schemas_with_buttons(update, context)
    elif text == "📊 Query Data":
        await show_query_menu(update, context)
    elif text == "🔥 Streaming Jobs":  # Add this
        await spark.check_streaming_jobs(update, context)
    elif text == "📈 Dashboards":
        await superset.list_dashboards(update, context)
    elif text == "❓ Help":
        await update.message.reply_text(
            "ℹ️ *About the Platform*\n"
            "/info - Get info about the platform\n\n"
            "🔧 *Airflow Commands:*\n"
            "/dags - List all DAGs\n"
            "/run <dag_id> - Trigger a DAG\n"
            "/status <dag_id> - Check DAG status\n"
            "/recent - Recent DAG runs\n\n"
            "📊 *Data Commands:*\n"
            "/schemas - List available schemas\n"
            "/tables <schema> - List tables in schema\n"
            "/query <sql> - Run SQL query\n"
            "/count <table> - Count rows in table\n\n"
            "🤖 *AI Assistant:*\n"
            "/ai - Start AI assistant mode\n"
            "Chat naturally to control everything!\n\n"
            "Or use the buttons!",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("Unknown option. Please use the buttons or /help")


async def show_dags_with_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        session = airflow.get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags"
        response = session.get(url, params={'limit': 100})
        response.raise_for_status()
        
        dags = response.json().get('dags', [])
        
        if not dags:
            await update.message.reply_text("No DAGs found.")
            return
        
        keyboard = []
        for dag in dags[:20]:
            dag_id = dag['dag_id']
            is_paused = "⏸️" if dag['is_paused'] else "▶️"
            keyboard.append([
                InlineKeyboardButton(f"{is_paused} {dag_id}", callback_data=f"dag_menu_{dag_id}")
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Select a DAG:", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error listing DAGs: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def show_schemas_with_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = trino_queries.get_trino_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"SHOW SCHEMAS IN {config.TRINO_CATALOG}")
        schemas = cursor.fetchall()
        
        if not schemas:
            await update.message.reply_text("No schemas found.")
            return
        
        keyboard = []
        for schema in schemas:
            SCHEMA_NAME = schema[0]
            if SCHEMA_NAME not in ['information_schema']:
                keyboard.append([
                    InlineKeyboardButton(f"📂 {SCHEMA_NAME}", callback_data=f"schema_{SCHEMA_NAME}")
                ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Select a schema:", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def show_query_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📂 Browse Schemas", callback_data="action_schemas")],
        [InlineKeyboardButton("📊 Browse Tables", callback_data="action_browse_tables")],
        [InlineKeyboardButton("🔍 Custom Query", callback_data="action_custom_query")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("What would you like to do?", reply_markup=reply_markup)