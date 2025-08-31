import os
import json
import requests
from pathlib import Path
from typing import Optional
from datetime import datetime

class SlackNotifier:
    def __init__(self, webhook_url: Optional[str] = None):
        """Initialize Slack notifier with webhook URL from environment or direct input."""
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        if not self.webhook_url:
            raise ValueError("Slack webhook URL not provided and SLACK_WEBHOOK_URL environment variable not set")

    def send_notification(self, message: str, color: str = "good") -> bool:
        """
        Send a notification to Slack.
        
        Args:
            message: The message to send
            color: Slack attachment color ("good"=green, "warning"=yellow, "danger"=red)
        """
        try:
            payload = {
                "attachments": [{
                    "color": color,
                    "text": message,
                    "footer": "DB Backup Manager",
                    "ts": int(datetime.now().timestamp())
                }]
            }
            
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            print(f"Failed to send Slack notification: {str(e)}")
            return False

def notify_backup_status(
    backup_type: str,
    backup_file: Path,
    success: bool,
    upload_status: Optional[dict] = None
) -> None:
    """
    Send a notification about backup status to Slack.
    
    Args:
        backup_type: 'full' or 'incremental'
        backup_file: Path to the backup file
        success: Whether the backup was successful
        upload_status: Optional dict with cloud upload status
    """
    try:
        notifier = SlackNotifier()
        
        size_mb = backup_file.stat().st_size / (1024 * 1024) if backup_file.exists() else 0
        
        if success:
            color = "good"
            status = "Successful"
        else:
            color = "danger"
            status = "Failed"
            
        message = (
            f"*Database Backup Status*\n"
            f"Type: {backup_type.title()}\n"
            f"Status: {status}\n"
            f"File: `{backup_file.name}`\n"
            f"Size: {size_mb:.2f} MB\n"
        )
        

        if upload_status:
            cloud_status = []
            for provider, status in upload_status.items():
                icon = "Success" if status else "Failure"
                cloud_status.append(f"{provider}: {icon}")
            message += f"Cloud Upload: {' | '.join(cloud_status)}\n"
        
        notifier.send_notification(message, color)
        
    except Exception as e:
        print(f"Error sending notification: {str(e)}")


if __name__ == "__main__":

    test_file = Path("test_backup.sql.gz")
    notify_backup_status(
        backup_type="full",
        backup_file=test_file,
        success=True,
        upload_status={"S3": True, "GCS": False}
    )
