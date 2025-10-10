import imaplib
import email
from email.header import decode_header
from collections import defaultdict
from datetime import datetime, timezone

# ... mevcut .env yükleme ve IMAP değişkenlerin burada ...
load_dotenv()

IMAP_SERVER = os.getenv("IMAP_SERVER")
IMAP_PORT = int(os.getenv("IMAP_PORT") or 993)
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

def _decode_subject(raw_subj):
    try:
        if raw_subj is None:
            return "No Subject"
        parts = decode_header(raw_subj)
        decoded = []
        for text, enc in parts:
            if isinstance(text, bytes):
                decoded.append(text.decode(enc or "utf-8", errors="ignore"))
            else:
                decoded.append(text)
        s = " ".join(decoded).strip()
        return s if s else "No Subject"
    except Exception:
        return "No Subject"

def connect_imap():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select("INBOX")
    return mail

def daily_summary(only_today=False, max_subjects_per_day=1000):
    """
    INBOX'taki mailleri tarih bazında gruplayıp:
      - Günlük toplam adedi
      - İlgili gün için konu başlıklarını
    yazdırır.
    only_today=True verilirse sadece bugünü özetler.
    """
    mail = connect_imap()

    # Tüm ID'leri al (sadece header fetch edeceğiz)
    status, messages = mail.search(None, "ALL")
    if status != "OK":
        print("❌ Arama başarısız.")
        mail.logout()
        return

    ids = messages[0].split()
    if not ids:
        print("❌ Gelen kutusunda e-posta yok.")
        mail.logout()
        return

    per_day = defaultdict(list)

    # Sadece gerekli header alanlarını çek: DATE ve SUBJECT
    # BODY.PEEK kullanarak "okunmadı" durumunu etkilemeyiz.
    for msg_id in ids:
        status, data = mail.fetch(msg_id, '(BODY.PEEK[HEADER.FIELDS (DATE SUBJECT)])')
        if status != "OK" or not data or not isinstance(data[0], tuple):
            continue

        raw_headers = data[0][1]
        msg = email.message_from_bytes(raw_headers)

        raw_date = msg.get("Date")
        raw_subj = msg.get("Subject")

        # Tarihi güvenle YYYY-MM-DD yap
        try:
            dt = email.utils.parsedate_to_datetime(raw_date) if raw_date else None
            if dt and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = None

        day_key = (dt.astimezone().strftime("%Y-%m-%d")) if dt else "Unknown-Date"
        subj = _decode_subject(raw_subj)

        per_day[day_key].append(subj)

    mail.logout()

    # Sadece bugün istenirse filtrele
    if only_today:
        today = datetime.now().strftime("%Y-%m-%d")
        per_day = {k: v for k, v in per_day.items() if k == today}

    if not per_day:
        print("ℹ️ Seçili gün(ler) için kayıt yok.")
        return

    # Tarihe göre sıralı yazdır (yeni → eski, Unknown en sonda)
    def sort_key(k):
        return (k == "Unknown-Date", k)  # Unknown-Date en sona
    for day in sorted(per_day.keys(), key=sort_key, reverse=True):
        subjects = per_day[day]
        print(f"\n📅 {day} — toplam: {len(subjects)} mail")
        # Konuları yazdır
        for i, s in enumerate(subjects[:max_subjects_per_day], start=1):
            print(f"  {i:02d}. {s}")

if __name__ == "__main__":
    # 1) Tüm günler için özet:
    daily_summary(only_today=False)

    # 2) Sadece bugünün özeti:
    # daily_summary(only_today=True)
