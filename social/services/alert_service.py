"""
Alert service for email notifications.
"""

import ntpath
import smtplib
from email.message import EmailMessage
from typing import Optional

from shared.utils.env import get_env


class Alert:
    """
    Email alert service.
    Sends notifications for errors and status updates.
    """

    def __init__(self, sender: str, receiver: str):
        """
        Initialize alert service.

        Args:
            sender: Sender email address
            receiver: Receiver email address
        """
        self.sender = sender
        self.receiver = receiver
        self.email = self._set_email_obj(From=self.sender, To=self.receiver)

    def _set_email_obj(self, From: str, To: str) -> EmailMessage:
        """Create email message object."""
        msg = EmailMessage()
        msg["From"] = From
        msg["To"] = To
        return msg

    def set_email_content(
        self,
        subject: str,
        content: str,
        loc_file_attach: Optional[str] = None,
    ) -> None:
        """
        Set email subject and content.

        Args:
            subject: Email subject
            content: Email body (HTML)
            loc_file_attach: Optional file attachment path
        """
        self.email["Subject"] = subject
        self.email.set_content(content, subtype="html")

        if loc_file_attach:
            with open(loc_file_attach, "r") as file:
                f = file.read()
                self.email.add_attachment(f, filename=ntpath.basename(loc_file_attach))

    def send_email(self) -> None:
        """Send the email."""
        smtp_host = get_env("SMTP_HOST", "localhost")
        smtp_port = int(get_env("SMTP_PORT", "25"))

        server = smtplib.SMTP()
        server.connect(host=smtp_host, port=smtp_port)
        server.set_debuglevel(1)
        server.send_message(self.email)
        server.quit()


def send_alert(
    subject: str,
    content: str,
    receiver: str = "DataScience@esprinet.com",
    loc_file_attach: Optional[str] = None,
) -> None:
    """
    Send an alert email.

    Args:
        subject: Email subject
        content: Email body
        receiver: Recipient email
        loc_file_attach: Optional attachment
    """
    sender = get_env("ALERT_SENDER", "fremont.esprinet.com")

    alert = Alert(sender=sender, receiver=receiver)
    alert.set_email_content(
        subject=subject,
        content=content,
        loc_file_attach=loc_file_attach,
    )
    alert.send_email()
