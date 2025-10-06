import logging, trino, config
from telegram import Update
from telegram.ext import ContextTypes


logger = logging.getLogger(__name__)


def get_trino_connection():
    return trino.dbapi.connect(
        host=config.TRINO_HOST,
        port=config.TRINO_PORT,
        user=config.TRINO_USER,
        catalog=config.TRINO_CATALOG
    )


async def list_schemas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"SHOW SCHEMAS IN {config.TRINO_CATALOG}")
        schemas = cursor.fetchall()
        
        if not schemas:
            await update.message.reply_text("No schemas found.")
            return
        
        message = f"📂 *Schemas in {config.TRINO_CATALOG}:*\n\n"
        for schema in schemas:
            SCHEMA_NAME = schema[0]
            if SCHEMA_NAME not in ['information_schema']:
                message += f"• `{SCHEMA_NAME}`\n"
        
        message += f"\nUse `/tables <schema>` to list tables"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def list_tables(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /tables <SCHEMA_NAME>")
        return
    
    schema = context.args[0]
    
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"SHOW TABLES IN {config.TRINO_CATALOG}.{schema}")
        tables = cursor.fetchall()
        
        if not tables:
            await update.message.reply_text(f"No tables found in schema `{schema}`.")
            return
        
        message = f"📊 *Tables in {schema}:*\n\n"
        for table in tables:
            table_name = table[0]
            message += f"• `{table_name}`\n"
        
        message += f"\nUse `/count {schema}.table_name` to count rows"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def run_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: /query SELECT * FROM schema.table LIMIT 10"
        )
        return
    
    sql = ' '.join(context.args)
    
    if not sql.strip().upper().startswith('SELECT'):
        await update.message.reply_text("❌ Only SELECT queries are allowed.")
        return
    
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        if not rows:
            await update.message.reply_text("Query returned no results.")
            return
        
        message = f"📊 *Query Results:*\n\n"
        message += f"```\n"
        
        message += " | ".join(columns) + "\n"
        message += "-" * 50 + "\n"
        
        for row in rows[:10]:
            message += " | ".join(str(val) for val in row) + "\n"
        
        if len(rows) > 10:
            message += f"\n... and {len(rows) - 10} more rows"
        
        message += "```"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error running query: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def count_rows(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /count schema.table_name")
        return
    
    table = context.args[0]
    
    if '.' not in table:
        await update.message.reply_text("❌ Please specify table as: schema.table_name")
        return
    
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        query = f"SELECT COUNT(*) FROM {config.TRINO_CATALOG}.{table}"
        cursor.execute(query)
        
        count = cursor.fetchone()[0]
        
        await update.message.reply_text(
            f"📊 Table `{table}` has *{count:,}* rows",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error counting rows: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")