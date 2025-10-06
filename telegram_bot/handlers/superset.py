import logging, requests, config
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

def get_superset_token():
    """Get access token from Superset."""
    try:
        response = requests.post(
            f"{config.SUPERSET_URL}/api/v1/security/login",
            json={
                "username": config.SUPERSET_USER,
                "password": config.SUPERSET_PASSWORD,
                "provider": "db",
                "refresh": True
            },
            timeout=5
        )
        response.raise_for_status()
        return response.json().get('access_token')
    except Exception as e:
        logger.error(f"Error getting Superset token: {e}")
        return None


async def list_dashboards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List available Superset dashboards."""
    try:
        token = get_superset_token()
        if not token:
            await update.message.reply_text("❌ Failed to authenticate with Superset")
            return
        
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(
            f"{config.SUPERSET_URL}/api/v1/dashboard/",
            headers=headers,
            timeout=5
        )
        response.raise_for_status()
        
        dashboards = response.json().get('result', [])
        
        if not dashboards:
            await update.message.reply_text("No dashboards found.")
            return
        
        message = "📊 Available Dashboards:\n\n"
        for dash in dashboards[:10]:
            title = dash.get('dashboard_title', 'Untitled')
            dash_id = dash.get('id')
            url = f"{config.SUPERSET_PUBLIC_URL}/superset/dashboard/{dash_id}/"
            message += f"• {title}\n  {url}\n\n"
        
        await update.message.reply_text(message)
    except Exception as e:
        logger.error(f"Error listing dashboards: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")