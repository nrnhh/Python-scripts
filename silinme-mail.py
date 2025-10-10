#!/usr/bin/env python3
import oracledb
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/var/log/deletion_report.log')

# Oracle database connection parameters
DB_PARAMS = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:your_port/your_service_name'  # e.g., 'localhost:1521/orclpdb1'
}

# Email configuration
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SENDER_EMAIL = 'your_email@gmail.com'
SENDER_PASSWORD = 'your_app_password'  # Use App Password for Gmail
RECIPIENT_EMAIL = 'recipient@example.com'
EMAIL_SUBJECT = 'Daily Deleted Services Report'

def fetch_deleted_services():
    try:
        # Connect to the Oracle database
        conn = oracledb.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Calculate date range for the past 24 hours
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)

        # SQL query with dynamic date range
        query = """
        SELECT 
            y.adsoyad AS "Silen user",
            COUNT(*) AS "Silinmiş xidmət sayı",
            SUM(z.is_ucret) AS "Ümumi xidmət məbləği"
        FROM fonethbys.h_hastakayit t 
        LEFT JOIN fonethbys.log_h_hastakayit_alt_del r ON r.hi_kayitid = t.hk_id
        LEFT JOIN fonethbysadm.ytk_kullanici y ON r.log_userid = y.id
        LEFT JOIN fonethbys.h_islem z ON z.is_id = r.hi_islemid
        WHERE r.hi_tarih BETWEEN :start_date AND :end_date
        GROUP BY y.adsoyad
        ORDER BY y.adsoyad
        """

        # Execute query with bind variables
        cursor.execute(query, {
            'start_date': start_time.strftime('%d.%m.%Y'),
            'end_date': end_time.strftime('%d.%m.%Y')
        })
        results = cursor.fetchall()

        # Close database connection
        cursor.close()
        conn.close()
        logging.info("Successfully fetched data from database")
        return results
    except oracledb.Error as e:
        logging.error(f"Database error: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return []

def send_email(results):
    # Create email message
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = EMAIL_SUBJECT

    # Create HTML table for email body
    html = """
    <html>
    <body>
    <h2>Daily Deleted Services Report</h2>
    <p>Below is the report of services deleted in the past 24 hours as of {}</p>
    <table border="1" style="border-collapse: collapse;">
        <tr>
            <th>Silen user</th>
            <th>Silinmiş xidmət sayı</th>
            <th>Ümumi xidmət məbləği</th>
        </tr>
    """.format(datetime.now().strftime('%d.%m.%Y %H:%M:%S'))

    for row in results:
        html += f"""
        <tr>
            <td>{row[0]}</td>
            <td>{row[1]}</td>
            <td>{row[2] if row[2] is not None else '0'}</td>
        </tr>
        """
    
    html += """
    </table>
    </body>
    </html>
    """

    # Attach HTML to email
    msg.attach(MIMEText(html, 'html'))

    try:
        # Connect to SMTP server
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Email error: {e}")

if __name__ == "__main__":
    logging.info(f"Running job at {datetime.now()}")
    results = fetch_deleted_services()
    if results:
        send_email(results)
    else:
        logging.info("No data to send")