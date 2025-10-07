"""
Spark streaming job monitoring handlers for Telegram bot.

This module provides async command handlers for monitoring Apache Spark streaming
jobs running in a Docker container. It checks which predefined streaming jobs are
currently active by inspecting container processes.

Commands:
    /jobs - Check which streaming jobs are currently running in the Spark container

The module executes Docker commands to inspect the Spark container's processes and
matches them against a predefined list of streaming job names from constants. Results
are formatted with status indicators and include error handling with logging.
"""

import logging, subprocess, constants
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
        
        for line in lines:
            for job in constants.STREAMING_JOBS:
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