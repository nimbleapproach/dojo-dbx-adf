# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
working_folder = '/Workspace' + current_notebook[0:current_notebook.rfind('/')]
report_folder = working_folder + "/reports"

available_report_files = sorted(os.listdir(report_folder))

# Prompting input parameters
try:
    REPORT_NAME = dbutils.widgets.get("wg_reportName")
except:
    dbutils.widgets.text(name="wg_reportName", defaultValue="")
    REPORT_NAME = dbutils.widgets.get("wg_reportName")

try:
    REPORT_DATE = dbutils.widgets.get("wg_reportDate")
except:
    dbutils.widgets.text(name="wg_reportDate", defaultValue="")
    REPORT_DATE = dbutils.widgets.get("wg_reportDate")

try:
    REPORT_FILE = dbutils.widgets.get("wg_reportFile")
except:
    dbutils.widgets.dropdown(name="wg_reportFile", defaultValue=available_report_files[0], choices=available_report_files)
    REPORT_FILE = dbutils.widgets.get("wg_reportFile")

try:
    SENDER = dbutils.widgets.get("wg_sender")
except:
    dbutils.widgets.text(name="wg_sender", defaultValue="")
    SENDER = dbutils.widgets.get("wg_sender")

try:
    RECEIVER = dbutils.widgets.get("wg_receiver")
except:
    dbutils.widgets.text(name="wg_receiver", defaultValue="")
    RECEIVER = dbutils.widgets.get("wg_receiver")

try:
    CC = dbutils.widgets.get("wg_cc")
except:
    dbutils.widgets.text(name="wg_cc", defaultValue="")
    CC = dbutils.widgets.get("wg_cc")

try:
    BCC = dbutils.widgets.get("wg_bcc")
except:
    dbutils.widgets.text(name="wg_bcc", defaultValue="")
    BCC = dbutils.widgets.get("wg_bcc")

try:
    SMTP_HOST = dbutils.widgets.get("wg_smtp_host")
except:
    dbutils.widgets.text(name="wg_smtp_host", defaultValue="")
    SMTP_HOST = dbutils.widgets.get("wg_smtp_host")

try:
    SMTP_PORT = dbutils.widgets.get("wg_smtp_port")
except:
    dbutils.widgets.text(name="wg_smtp_port", defaultValue="")
    SMTP_PORT = dbutils.widgets.get("wg_smtp_port")

try:
    SMTP_PASSWORD = dbutils.widgets.get("wg_smtp_password")
except:
    dbutils.widgets.text(name="wg_smtp_password", defaultValue="")
    SMTP_PASSWORD = dbutils.widgets.get("wg_smtp_password")

# COMMAND ----------

import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Create a multipart message and set headers
subject = REPORT_NAME

message = MIMEMultipart()
message["From"] = SENDER
message["To"] = RECEIVER
message["CC"] = CC
message["Subject"] = subject

recipients=f"{RECEIVER},{CC},{BCC}".split(",")
# print(recipients)

# body message
body = f"{REPORT_NAME} extract for {REPORT_DATE}"

# convert the body to a MIME compatible string
message.attach(MIMEText(body, "plain"))

# Open the attachment file in bynary
attachment_path = report_folder + "/" + REPORT_FILE
with open(attachment_path, "rb") as attachment:
    # Add file as application/octet-stream
    part = MIMEApplication(attachment.read(), _subtype="xlsx")

# Add header with pdf name
part.add_header(
    "Content-Disposition",
    f"attachment; filename={REPORT_FILE}",
)

# Add attachment to message and convert message to string
message.attach(part)

with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
    server.starttls()
    server.login(SENDER, SMTP_PASSWORD)
    server.send_message(message, to_addrs=recipients)
