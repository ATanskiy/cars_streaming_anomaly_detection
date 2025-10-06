"""
Telegram notification helper for alert aggregation batches.
Formats aggregated alert data into structured Telegram messages with
emojis and markdown formatting, then sends them to a specified chat.
Reads bot token from environment and chat ID from command line argument.
Used by the alert counter job's foreachBatch processing.
"""

import sys, os, requests

# Get chat_id from command line argument (first argument passed to the script)
telegram_chat_id = sys.argv[1] #if len(sys.argv) > 1 else "690514325"
telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")

def send_telegram_message(chat_id, message):
    """Send message to specific chat"""
    if not chat_id:
        print("No chat_id provided, skipping Telegram notification")
        return
    
    url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print(f"✓ Sent message to Telegram")
    except Exception as e:
        print(f"✗ Error sending to Telegram: {e}")

def format_message(row):
    window_start = row['window']['start']
    window_end = row['window']['end']
    
    return (
        f"🚗 *Alert Aggregation*\n\n"
        f"⏰ {window_start} → {window_end}\n\n"
        f"📊 Total: `{row['num_of_rows']}`\n"
        f"🎨 Black: `{row['num_of_black']}` | White: `{row['num_of_white']}` | Silver: `{row['num_of_silver']}`\n\n"
        f"🏎️ Max Speed: `{row['maximum_speed']}` | Gear: `{row['maximum_gear']}` | RPM: `{row['maximum_rpm']}`"
    )

# Send to Telegram using foreachBatch
def send_batch(batch_df, batch_id):
    rows = batch_df.collect()
    print(f"Processing batch {batch_id} with {len(rows)} rows")
    for row in rows:
        message = format_message(row)
        send_telegram_message(telegram_chat_id, message)