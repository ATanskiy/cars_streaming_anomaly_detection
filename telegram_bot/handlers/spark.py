import logging
import subprocess
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def check_streaming_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check which streaming jobs are running in Spark container."""
    try:
        result = subprocess.run(
            ["docker", "exec", "spark", "ps", "aux"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            await update.message.reply_text(f"❌ Error: {result.stderr}")
            return
        
        lines = result.stdout.split('\n')
        streaming_jobs = set()
        
        job_patterns = [
            "4_data_generator",
            "5_enriching",
            "6_alerting",
            "7_print_aggregations"
        ]
        
        for line in lines:
            for job in job_patterns:
                if job in line:
                    streaming_jobs.add(job)
                    break
        
        if not streaming_jobs:
            message = "⚠️ No streaming jobs currently running"
        else:
            message = "✅ *Running Streaming Jobs:*\n\n"
            for job in sorted(streaming_jobs):
                escaped_job = job.replace('_', '\\_')
                message += f"▶️ {escaped_job}\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error checking jobs: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")