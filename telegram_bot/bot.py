"""
Main Telegram bot application for data platform management.
Provides interactive command interface and menu-driven UI for managing
Airflow DAGs, running Trino queries, monitoring Spark streaming jobs,
and accessing Superset dashboards. Handles user commands, button callbacks,
and error handling with comprehensive logging.
"""

import logging, config
from telegram import Update
from handlers import airflow, trino_queries, buttons, spark, superset, ai_agent
from telegram.ext import Application, CommandHandler, MessageHandler, \
      CallbackQueryHandler, filters
from start_functions import start, help_command, error_handler, handle_menu_button, \
    show_about


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def main():
    application = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("info", show_about))

    # Airflow commands
    application.add_handler(CommandHandler("dags", airflow.list_dags))
    application.add_handler(CommandHandler("run", airflow.trigger_dag))
    application.add_handler(CommandHandler("status", airflow.dag_status))
    application.add_handler(CommandHandler("kill", airflow.kill_dag))
    application.add_handler(CommandHandler("recent", airflow.recent_runs))

    # Trino commands
    application.add_handler(CommandHandler("schemas", trino_queries.list_schemas))
    application.add_handler(CommandHandler("tables", trino_queries.list_tables))
    application.add_handler(CommandHandler("query", trino_queries.run_query))
    application.add_handler(CommandHandler("count", trino_queries.count_rows))

    # Spark Commands
    application.add_handler(CommandHandler("streaming", spark.check_streaming_jobs))
    application.add_handler(CommandHandler("dashboards", superset.list_dashboards))

    # AI agent
    application.add_handler(CommandHandler("ai", ai_agent.start_ai_mode))

    
    application.add_handler(CallbackQueryHandler(buttons.button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_button))

    application.add_error_handler(error_handler)

    logger.info("Starting bot...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()