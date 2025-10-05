import logging, requests, config
from telegram import Update
from telegram.ext import ContextTypes
from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)


def get_airflow_session():
    session = requests.Session()
    session.auth = HTTPBasicAuth(config.AIRFLOW_USERNAME, config.AIRFLOW_PASSWORD)
    session.headers.update({'Content-Type': 'application/json'})
    return session


async def list_dags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        session = get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags"
        
        response = session.get(url, params={'limit': 100})
        response.raise_for_status()
        
        dags = response.json().get('dags', [])
        
        if not dags:
            await update.message.reply_text("No DAGs found.")
            return
        
        message = "📋 *Available DAGs:*\n\n"
        for dag in dags:
            dag_id = dag['dag_id']
            is_paused = "⏸️" if dag['is_paused'] else "▶️"
            message += f"{is_paused} `{dag_id}`\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error listing DAGs: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def trigger_dag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /run <dag_id>")
        return
    
    dag_id = context.args[0]
    
    try:
        session = get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        
        payload = {}  # Empty payload, let Airflow handle defaults
        
        response = session.post(url, json=payload)
        response.raise_for_status()
        
        run_data = response.json()
        run_id = run_data.get('dag_run_id', 'unknown')
        
        await update.message.reply_text(
            f"✅ DAG `{dag_id}` triggered successfully!\n"
            f"Run ID: `{run_id}`\n\n"
            f"Check status with: /status {dag_id}",
            parse_mode='Markdown'
        )
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            await update.message.reply_text(f"❌ DAG `{dag_id}` not found.")
        else:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error triggering DAG: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def dag_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /status <dag_id>")
        return
    
    dag_id = context.args[0]
    
    try:
        session = get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns"
        
        response = session.get(url, params={'limit': 5, 'order_by': '-execution_date'})
        response.raise_for_status()
        
        runs = response.json().get('dag_runs', [])
        
        if not runs:
            await update.message.reply_text(f"No runs found for DAG `{dag_id}`.")
            return
        
        message = f"📊 *Status for DAG:* `{dag_id}`\n\n"
        
        for run in runs:
            state = run['state']
            emoji = {
                'success': '✅',
                'running': '🔄',
                'failed': '❌',
                'queued': '⏳'
            }.get(state, '❓')
            
            exec_date = run['execution_date'][:19]
            message += f"{emoji} {state.upper()}\n"
            message += f"   Execution: {exec_date}\n\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            await update.message.reply_text(f"❌ DAG `{dag_id}` not found.")
        else:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error checking DAG status: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def recent_runs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        session = get_airflow_session()
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/~/dagRuns/list"
        
        payload = {
            "page_limit": 10,
            "order_by": "-execution_date"
        }
        
        response = session.post(url, json=payload)
        response.raise_for_status()
        
        runs = response.json().get('dag_runs', [])
        
        if not runs:
            await update.message.reply_text("No recent runs found.")
            return
        
        message = "🕐 *Recent DAG Runs:*\n\n"
        
        for run in runs:
            dag_id = run['dag_id']
            state = run['state']
            emoji = {
                'success': '✅',
                'running': '🔄',
                'failed': '❌',
                'queued': '⏳'
            }.get(state, '❓')
            
            exec_date = run['execution_date'][:19]
            message += f"{emoji} `{dag_id}`\n"
            message += f"   {state} - {exec_date}\n\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error fetching recent runs: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def kill_dag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /kill <dag_id> <run_id>")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Please provide both dag_id and run_id\nUsage: /kill <dag_id> <run_id>")
        return
    
    dag_id = context.args[0]
    run_id = context.args[1]
    
    try:
        session = get_airflow_session()
        # PATCH request to update the DAG run state to 'failed'
        url = f"{config.AIRFLOW_API_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        
        payload = {"state": "failed"}
        
        response = session.patch(url, json=payload)
        response.raise_for_status()
        
        await update.message.reply_text(
            f"🛑 DAG run killed successfully!\n"
            f"DAG: `{dag_id}`\n"
            f"Run ID: `{run_id}`",
            parse_mode='Markdown'
        )
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            await update.message.reply_text(f"❌ DAG `{dag_id}` or run `{run_id}` not found.")
        else:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error killing DAG: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")