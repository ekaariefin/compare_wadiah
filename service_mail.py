from email.message import EmailMessage
import smtplib
from config import SMTP_CONFIG

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg['From'] = SMTP_CONFIG['mail_sender']
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_CONFIG['mail_host'], SMTP_CONFIG['mail_port']) as server:
        server.starttls()
        server.login(SMTP_CONFIG['mail_sender'], SMTP_CONFIG['mail_password'])
        server.send_message(msg)
