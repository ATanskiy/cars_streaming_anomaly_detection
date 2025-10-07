"""
General utility handlers for Telegram bot.
Provides help, info, and welcome messages.
"""

import logging, constants
from telegram import Update
from handlers import menus, ai_agent
from telegram.ext import  ContextTypes

logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await menus.show_main_menu(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def handle_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    # Define which buttons should work even in AI mode (exit buttons)
    exit_keywords = ['exit', 'quit', 'stop', '/exit', '/stop', '/quit']
    
    # Check if user clicked a menu button (has emoji) or typed exit command
    is_menu_button = any(emoji in text for emoji in ['📋', '🕐', '📂', '📊', '🔥', '📈', '❓', '🤖'])
    is_exit_command = text.lower() in exit_keywords
    
    # If in AI mode AND it's not a menu button AND not an exit command -> route to AI
    if context.user_data.get('ai_mode', False) and not is_menu_button and not is_exit_command:
        await ai_agent.ai_chat(update, context)
    # If it's an exit command while in AI mode -> exit AI mode
    elif context.user_data.get('ai_mode', False) and is_exit_command:
        context.user_data['ai_mode'] = False
        context.user_data['ai_history'] = []
        await update.message.reply_text("👋 Exited AI mode. Buttons are active again!")
    # Otherwise, handle as normal menu button
    else:
        await menus.handle_button_text(update, context)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text(f"❌ An error occurred: {str(context.error)}")


async def show_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show project information and capabilities."""
    info_text = constants.INFO_DESCRIPTION
    
    await update.message.reply_text(info_text, parse_mode='Markdown')