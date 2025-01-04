# Databricks notebook source

# COMMAND ----------

import os
import smtplib
from email.message import EmailMessage

def sendEmail(subject: str, sender: str, recipients: list[str], cc: list[str], bcc: list[str], body: str,
              attachments: list[str], smtp_host: str, smtp_port: int, smtp_login: str, smtp_password: str):
    # Create a message and set headers

    recipients_as_string = ";".join(recipients)
    cc_as_string = ";".join(cc)
    bcc_as_string = ";".join(bcc)

    message = EmailMessage()
    message["From"] = sender
    message["To"] = recipients_as_string
    message["CC"] = cc_as_string
    message["Subject"] = subject

    # body message
    message.set_content(body)

    # Add attachments to message
    for attachment_path in attachments:
        file_name = attachment_path.split(os.sep)[-1]
        with open(attachment_path, "rb") as attachment:
            message.add_attachment(attachment.read(), maintype='application',
                                   subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename=file_name)

    all_recipients = f"{recipients_as_string};{cc_as_string};{bcc_as_string}".split(";")

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_login, smtp_password)
        server.send_message(message, to_addrs=all_recipients)
