import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import config
from handlers import airflow, trino_queries

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"Hi {user.first_name}!\n\n"
        "I'm your Cars Anomaly Detection Bot. Here's what I can do:\n\n"
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
        "/help - Show this message",
        parse_mode='Markdown'
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text(
            f"❌ An error occurred: {str(context.error)}"
        )


def main():
    application = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    
    application.add_handler(CommandHandler("dags", airflow.list_dags))
    application.add_handler(CommandHandler("run", airflow.trigger_dag))
    application.add_handler(CommandHandler("status", airflow.dag_status))
    application.add_handler(CommandHandler("recent", airflow.recent_runs))
    
    application.add_handler(CommandHandler("schemas", trino_queries.list_schemas))
    application.add_handler(CommandHandler("tables", trino_queries.list_tables))
    application.add_handler(CommandHandler("query", trino_queries.run_query))
    application.add_handler(CommandHandler("count", trino_queries.count_rows))

    application.add_error_handler(error_handler)

    logger.info("Starting bot...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()