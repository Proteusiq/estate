from os import environ
from typing import Any, Optional
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

SLACK_TOKEN = environ.get("SLACK_TOKEN")


def notify(
    text: str,
    channel: str,
    status: bool,
    username: Optional[str] = "airflow-bot",
    task_id: Optional[str] = "slack_notification",
    attachments: Optional[Any] = None,
):

    if status:
        status_emoji = ":tada:"
    else:
        status_emoji = ":skull_and_crossbones:"

    # place dict into a list
    if isinstance(attachments, dict):
        attachments = [attachments]

    return
    SlackAPIPostOperator(
        task_id=task_id,
        username=username,
        channel=channel,
        text=f"{status_emoji} {text}",
        attachments=attachments,
    ).execute()
