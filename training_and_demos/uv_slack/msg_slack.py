#!/usr/bin/env -S uv run --cache-dir /tmp/.uv_cache
# /// script
# dependencies = [
#   "slack_sdk==3.34.0",
#   "python-dotenv"
# ]
# ///
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import os

load_dotenv()
SLACK_TOKEN = os.getenv("SLACK_API_TOKEN")

def send_hello_world():
    # Initialize the Slack client with your bot token
    client = WebClient(token=SLACK_TOKEN)
    print("TEST")
    try:
        response = client.chat_postMessage(
            channel='#bot-aks-notifications',
            text='Hello, World! 👋'
        )
        print(f"Message sent successfully: {response['ts']}")

    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")

if __name__ == "__main__":
    send_hello_world()
