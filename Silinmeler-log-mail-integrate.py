# -*- coding: utf-8 -*-
"""
LOG Tablolarını İzleyen Script (Email Bildirimi ilə)
- Dəyişiklik olduqda BMP.az email göndərir (təkrar göndərməmək üçün state yenilənir).
- Hər dəqiqədə yoxlayır.
- Filtr: LOG_DATE > :last_date (LOG_DATE DATE tipi).
- Başlanğıc: 03.11.2025 00:00:00-dən sonra (köhnələr, məsələn 2012, reset olunur).
- JSON üçün default=str.
- Oracle 11g uyğunluğu: FETCH yox, ROWNUM ilə limit (subquery ilə).
"""

import oracledb
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import json
import os
from datetime import datetime

# ================== PARAMETRƏR ==================
DB_USER = os.getenv("ORA_USER", "NURAN")                        
DB_PASS = os.getenv("ORA_PASS", "Nuran..2024!!")               
DB_DSN  = os.getenv("ORA_DSN",  "172.18.79.23:1521/FONETAZ")    

INSTANT_CLIENT_DIR = r"C:\instant\instantclient_23_9"

# Email
SMTP_SERVER = "mail.bmp.az"
SMTP_PORT = 465
EMAIL_FROM = "nuran.hasanov@bmp.az"
EMAIL_PASSWORD = "N2024!H"
EMAIL_TO = "nuran.hasanov@bmp.az"

# Tablolar
TABLES = [
    ('FONETHBYS', 'LOG_AML_AMELIYAT_DEL'),
    ('FONETHBYS', 'LOG_H_HASTAKAYIT_ALT_DEL'),
    ('FONETHBYS', 'LOG_H_HASTAKAYIT_ALT_UPD'),
    ('FONETHBYS', 'LOG_H_HASTAKAYIT_DEL'),
    ('FONETHBYS', 'LOG_H_POLIKLINIK_DEL'),
    ('FONETHBYS', 'LOG_H_SEVK_DEL'),
    ('FONETHBYS', 'LOG_H_YHSERVIS_DEL'),
    ('FONETHBYS', 'LOG_KLINIK'),
    ('FONETHBYS', 'LOG_STOK_CIKISDETAY_SIL'),
    ('FONETHBYS', 'LOG_STOK_GIRISDETAY_SIL'),
    ('FONETHBYSADM', 'LOG_TANIM'),
    ('FONETHBYSADM', 'LOG_VEZNE'),
    ('FONETHBYSADM', 'LOG_VZN_GIRISLER_DEL'),
    ('FONETHBYSADM', 'LOG_VZN_GIRISLER_DETAY_DEL'),
    ('FONETHBYSADM', 'LOG_VZN_ODEMELER_DEL'),
    ('FONETHBYSADM', 'LOG_VZN_ODEMELER_DETAY_DEL'),
    ('FONETHBYSADM', 'LOG_VZN_TAHSILAT_ALT_DEL'),
    ('FONETHBYSADM', 'LOG_VZN_TAHSILAT_DEL')
]

STATE_FILE = 'log_monitor_state.json'
START_DATE = datetime(2025, 11, 3, 0, 0, 0)  # 03.11.2025 00:00:00

# Funksiyalar
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            # Köhnələri reset et (2012 kimi)
            for key in state:
                if state[key]:
                    try:
                        if isinstance(state[key], str):
                            loaded_date = datetime.strptime(state[key], '%Y-%m-%d %H:%M:%S')
                        elif isinstance(state[key], (int, float)):
                            from datetime import timedelta
                            base_date = datetime(1899, 12, 30)
                            loaded_date = base_date + timedelta(days=state[key])
                        else:
                            loaded_date = datetime.fromtimestamp(state[key])
                        if loaded_date < START_DATE:
                            print(f"Köhnə state reset: {key} - {loaded_date} -> {START_DATE}")
                            state[key] = START_DATE
                        else:
                            state[key] = loaded_date
                    except:
                        state[key] = START_DATE
                else:
                    state[key] = START_DATE
            return state
    return {f"{owner}.{table}": START_DATE for owner, table in TABLES}

def save_state(state):
    state_copy = {}
    for key, value in state.items():
        if isinstance(value, datetime):
            state_copy[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            state_copy[key] = value
    with open(STATE_FILE, 'w') as f:
        json.dump(state_copy, f, indent=4)

def connect_db():
    try:
        connection = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        return connection
    except oracledb.Error as e:
        print(f"DB xətası: {e}")
        return None

def get_new_rows(connection, owner, table, last_date=None):
    base_query = f"SELECT * FROM {owner}.{table} WHERE 1=1"
    params = {}
    if last_date is not None:
        base_query += " AND LOG_DATE > :last_date"
        params['last_date'] = last_date
    # ROWNUM ilə limit (Oracle 11g uyğun)
    query = f"""
    SELECT * FROM (
        {base_query} 
        ORDER BY LOG_DATE ASC
    ) WHERE ROWNUM <= 50
    """
    cursor = connection.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    # LOG_DATE-i str olaraq saxla
    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        if 'LOG_DATE' in row_dict and row_dict['LOG_DATE']:
            row_dict['LOG_DATE'] = str(row_dict['LOG_DATE'])
        result.append(row_dict)
    return result

def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    try:
        server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        server.quit()
        print(f"Email göndərildi: {subject}")
    except Exception as e:
        print(f"Email xətası: {e}")

def monitor_tables():
    state = load_state()
    conn = connect_db()
    if not conn:
        return

    # Köhnə state reset (2012 kimi köhnələr varsa)
    reset_needed = any(state[key] < START_DATE for key in state)
    if reset_needed:
        print("Köhnə state aşkarlandı (örn: 2012). Reset 03.11.2025 ilə...")
        for owner, table in TABLES:
            table_key = f"{owner}.{table}"
            state[table_key] = START_DATE
        save_state(state)
        print("Reset tamamlandı. Yalnız 03.11.2025-dən sonra olanlar izləniləcək.")

    changes_detected = False
    for owner, table in TABLES:
        table_key = f"{owner}.{table}"
        last_date = state[table_key]

        try:
            new_rows = get_new_rows(conn, owner, table, last_date)
            if new_rows:
                changes_detected = True
                # Yeni max LOG_DATE-i tap və yenilə
                dates = []
                for row in new_rows:
                    if row.get('LOG_DATE'):
                        try:
                            dates.append(datetime.strptime(row['LOG_DATE'], '%Y-%m-%d %H:%M:%S'))
                        except ValueError:
                            pass
                if dates:
                    new_last_date = max(dates)
                    state[table_key] = new_last_date
                    print(f"{table}: {len(new_rows)} yeni satır tapıldı, state yeniləndi: {new_last_date} (köhnə: {last_date})")

                body = f"Yeni dəyişiklik: {table} ({owner})\nVaxt: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                for row in new_rows:
                    body += f"Satır: {json.dumps(row, indent=2, ensure_ascii=False, default=str)}\n{'-'*50}\n"
                subject = f"LOG Dəyişikliyi: {table} ({len(new_rows)} yeni)"
                send_email(subject, body)
        except oracledb.Error as e:
            print(f"Xəta {table}-də yeni satır axtararkən: {e}. Tablo keçildi.")

    conn.close()
    save_state(state)
    if changes_detected:
        print("Bildirim göndərildi!")
    else:
        print("Dəyişiklik yoxdur.")

if __name__ == "__main__":
    try:
        oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_DIR)
        print("Instant Client OK!")
    except Exception as e:
        raise SystemExit(f"Instant Client xətası: {e}")
    
    while True:
        monitor_tables()
        time.sleep(60)
