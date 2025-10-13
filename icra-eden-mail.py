import imaplib
import email
from email.header import decode_header
from email.utils import parseaddr
from collections import Counter
from datetime import datetime
import pandas as pd  # Excel üçün lazım

# BMP.az IMAP konfiqurasiyası (sənin verdiyin SMTP-yə əsasən)
imap_server = "mail.bmp.az"
imap_port = 993  # SSL üçün standart
email_address = "nuran.hasanov@bmp.az"
password = "N2024!H"  # Sənin şifrən

# İcazəli göndərən email-lər (yalnız bunlardan gələnlər)
allowed_emails = ["hamid.abdulov@bmp.az", "feyruz.mirzayev@bmp.az", "parviz.aliyev@bmp.az"]

def connect_to_email():
    # SSL ilə qoşul
    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    mail.login(email_address, password)
    mail.select("INBOX")  # Inbox qovluğunu seç (gələn e-poçtlar)
    return mail

def search_emails_containing_word(mail, word="icra"):
    # "icra" sözünü ehtiva edən e-poçtları axtar (case-insensitive TEXT axtarışı, serverdə LOWER kimi işləyir)
    # %icra% kimi wildcards IMAP-də SUBSTRING ilə, amma sadə TEXT kifayətdir (boyuk/kiçik hərfləri əhatə edir)
    status, messages = mail.search(None, f'TEXT "{word}"')
    email_ids = messages[0].split()
    
    # Email ID-lərini int-ə çevir və tərs sırala (ən yenidən köhnəyə, çünki ID-lər artan)
    sorted_ids = sorted([int(id) for id in email_ids], reverse=True)
    total_count = len(sorted_ids)
    
    print(f"'{word}' sözünü ehtiva edən ümumi e-poçtların sayı (filterlənməmiş, boyuk/kiçik hərflər daxil): {total_count}")
    
    # Yalnız icazəli sender-lərdən olanları filter et
    senders = []
    email_details = []  # Detallar üçün saxla
    filtered_count = 0
    for num in sorted_ids:  # Yenidən köhnəyə
        try:
            status, msg_data = mail.fetch(str(num), "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            from_header = decode_header(msg["From"])[0][0]
            if isinstance(from_header, bytes):
                from_header = from_header.decode()
            realname, sender_email = parseaddr(from_header)
            if sender_email and sender_email.lower() in [e.lower() for e in allowed_emails]:  # Yalnız icazəli sender-lər
                filtered_count += 1
                # Göndərəni ad + email formatında saxla
                full_sender = f"{realname} <{sender_email}>" if realname else sender_email
                senders.append(full_sender)
                # Tarix və subject əlavə et (detallar üçün)
                date_header = msg["Date"]
                try:
                    date_obj = email.utils.parsedate_tz(date_header)
                    if date_obj:
                        date_str = datetime(*date_obj[:6]).strftime("%Y-%m-%d %H:%M")
                        ay = date_str[:7]  # YYYY-MM formatı
                    else:
                        date_str = date_header
                        ay = "Tarix yoxdur"
                except:
                    date_str = "Tarix yoxdur"
                    ay = "Tarix yoxdur"
                subject = decode_header(msg["Subject"])[0][0]
                if isinstance(subject, bytes):
                    subject = subject.decode()
                email_details.append({
                    'Tarix': date_str,
                    'Ay': ay,
                    'Göndərən (Ad və Email)': full_sender,
                    'Mövzu': subject
                })
        except Exception as fetch_error:
            print(f"Bu e-poçtu oxumaqda xəta (ID {num}): {fetch_error}")
            continue
    
    print(f"İcazəli göndərənlərdən filterlənmiş e-poçtların sayı: {filtered_count}")
    
    sender_counts = Counter(senders)
    
    # Aylara görə ümumi saylar (yalnız valide tarixlər)
    valid_details = [d for d in email_details if d['Ay'] != "Tarix yoxdur"]
    monthly_counts = Counter([d['Ay'] for d in valid_details])
    
    # Hər ay üçün sender saylarını hesabla
    monthly_sender_summary = []
    df_temp = pd.DataFrame(valid_details)
    for ay, group in df_temp.groupby('Ay'):
        ay_sender_counts = Counter(group['Göndərən (Ad və Email)'])
        for sender, count in ay_sender_counts.most_common():
            monthly_sender_summary.append({
                'Ay': ay,
                'Göndərən (Ad və Email)': sender,
                'Say': count
            })
    
    # Nəticələri çap et (ən çoxdan az-a doğru sırala)
    print("\nHər icazəli göndərəndən gələn e-poçtların sayı:")
    for sender, count in sender_counts.most_common():
        print(f"{sender}: {count} ədəd")
    
    # Aylara görə ümumi saylar (ən çoxdan az-a doğru)
    print("\nAylara görə ümumi e-poçt sayları:")
    for ay, count in monthly_counts.most_common():
        print(f"{ay}: {count} ədəd")
    
    # Hər ay üçün sender sayları
    print("\nHər ay üçün icazəli göndərənlərə görə saylar (ən çoxdan az-a):")
    monthly_df = pd.DataFrame(monthly_sender_summary)
    monthly_df = monthly_df.sort_values(by=['Ay', 'Say'], ascending=[False, False])
    for ay in sorted(monthly_df['Ay'].unique(), reverse=True):  # Yenidən köhnəyə
        ay_group = monthly_df[monthly_df['Ay'] == ay]
        print(f"\n--- {ay} ayı (ümumi: {monthly_counts[ay]} ədəd) ---")
        for _, row in ay_group.iterrows():
            print(f"  {row['Göndərən (Ad və Email)']}: {row['Say']} ədəd")
    
    # Ümumi siyahı (bütün e-poçtların detalları, ən yenidən köhnəyə, aylara görə qruplaşdırılıb)
    print("\nÜmumi siyahı (aylara görə qruplaşdırılıb, hər ayın e-poçtları):")
    df_temp = pd.DataFrame(email_details)
    df_temp = df_temp.sort_values(by=['Ay', 'Tarix'], ascending=False)  # Ay və tarixə görə sırala (yenidən köhnəyə)
    for ay, group in df_temp.groupby('Ay'):
        if ay != "Tarix yoxdur":
            print(f"\n--- {ay} ayı ({len(group)} ədəd) ---")
            for _, detail in group.iterrows():
                print(f"Tarix: {detail['Tarix']} | Göndərən: {detail['Göndərən (Ad və Email)']} | Mövzu: {detail['Mövzu']}")
    
    # Excel faylına saxla (Ay sütunu və Göndərən adı ilə, aylara görə sırala)
    if email_details:
        df = pd.DataFrame(email_details)
        df = df.sort_values(by=['Ay', 'Tarix'], ascending=False)
        excel_file = 'icra_case_insensitive_filtered_emails_by_month_with_sender_counts.xlsx'
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Bütün E-poçtlar', index=False)
            # Aylara görə ümumi summary sheet
            summary_df = pd.DataFrame(monthly_counts.items(), columns=['Ay', 'Say'])
            summary_df = summary_df.sort_values(by='Say', ascending=False)
            summary_df.to_excel(writer, sheet_name='Aylara Görə Ümumi Summary', index=False)
            # Hər ay üçün sender summary sheet
            monthly_sender_df = pd.DataFrame(monthly_sender_summary)
            monthly_sender_df = monthly_sender_df.sort_values(by=['Ay', 'Say'], ascending=[False, False])
            monthly_sender_df.to_excel(writer, sheet_name='Aylara Görə Sender Summary', index=False)
        print(f"\nSiyahı Excel faylına saxlanıldı: {excel_file} (Bütün E-poçtlar, Ümumi Summary və Sender Summary sheet-lər)")
    else:
        print("\nSaxlanacaq məlumat yoxdur.")
    
    return filtered_count, sender_counts, monthly_counts, monthly_sender_summary, email_details

# Skripti işə sal
try:
    mail = connect_to_email()
    total, sender_counts, monthly_counts, monthly_summary, details = search_emails_containing_word(mail)
    mail.close()
    mail.logout()
except Exception as e:
    print(f"Xəta: {e}")
    print("IMAP ayarlarını və şifrəni yoxla! BMP.az dəstəyi ilə əlaqə saxla əgər qoşulma problemi varsa.")
