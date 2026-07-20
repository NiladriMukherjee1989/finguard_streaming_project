from pyspark import pipelines as dp


# Gmail SMTP Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "payeld20@gmail.com"
RECIPIENT_EMAIL = "payeld20@gmail.com"
APP_PASSWORD = "sazo ejgm jsej kahf"


def _build_email_body(rows):
    """Format alert rows into a plain-text email body."""
    body = "The following high-value transactions have been detected:\n\n"
    body += "-" * 60 + "\n"
    for row in rows:
        body += f"Alert ID        : {row['Alert_ID']}\n"
        body += f"Transaction ID  : {row['Transaction_ID']}\n"
        body += f"Customer ID     : {row['Customer_ID']}\n"
        body += f"Amount          : {row['Transaction_Amount']} {row['currency']}\n"
        body += f"Transaction Date: {row['Transaction_Date']}\n"
        body += f"Alert Type      : {row['Alert_Type']}\n"
        body += f"Alert Timestamp : {row['Alert_Timestamp']}\n"
        body += "-" * 60 + "\n"
    body += f"\nTotal alerts in this batch: {len(rows)}\n"
    body += "\n-- FinGuard Streaming Pipeline"
    return body


def send_alert_email(rows):
    """Send email with high-value transaction alerts via Gmail."""
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    import ssl
    import socket

    if not rows:
        return

    subject = f"[FinGuard Alert] {len(rows)} High Value Transaction(s) Detected"
    body = _build_email_body(rows)

    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Connect to Gmail and send
    _send_via_gmail(msg)


def _send_via_gmail(msg):
    """Dispatch composed message through Gmail SMTP (port 587, TLS)."""
    import smtplib
    conn = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    conn.starttls()
    conn.login(SENDER_EMAIL, APP_PASSWORD)
    conn.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
    conn.quit()


@dp.foreach_batch_sink(name="gmail_alert_sink")
def gmail_alert_sink(df, batch_id):
    """Process each micro-batch and send email alerts for new high-value transactions."""
    rows = [row.asDict() for row in df.collect()]
    if rows:
        send_alert_email(rows)


@dp.append_flow(target="gmail_alert_sink")
def high_value_trans_alert_flow():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("finguard.gold.detect_high_value_trans")
    )
