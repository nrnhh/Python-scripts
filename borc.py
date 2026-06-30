import pandas as pd
import oracledb
import os
import warnings
import smtplib
import asyncio
from datetime import datetime
from email.message import EmailMessage
from openpyxl.styles import Font, Border, Side, Alignment
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

# Texniki xəbərdarlıqları gizlədirik ki, ekran təmiz qalsın
warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
#                               KONFİQURASİYA
# ==============================================================================
ALLOWED_CHAT_ID = -1003875155955
BOT_TOKEN = "8756780809:AAGaIupV7sZeZ0U8dKa8LSW4zyf6oeUFoi4"

# Verilənlər Bazası Tənzimləmələri
DB_USER = "NURAN"
DB_PASS = "Nuran..2024!!"
DB_DSN = "172.18.79.23:1521/FONETAZ"

# UBUNTU ÜÇÜN ORACLE CLIENT YOLU
INSTANT_CLIENT_DIR = "/opt/oracle/instantclient_23_6"

# Mail Tənzimləmələri (BMP Mail Server)
SMTP_SERVER = "mail.bmp.az"
SMTP_PORT = 587
SENDER_EMAIL = "nuran.hasanov@bmp.az"
SENDER_PASS = "N2024!H"

# Mail Alıcıları
MAIL_TO = "murad.zengiyev@bmp.az"
MAIL_CC = [
    "farid.gasimov@bmp.az",
    "kamil.shiraliyev@azeraholding.az",
    "nuran.hasanov@bmp.az",
    "nuranhasanov2003@gmail.com"
]

# Oracle Client inisializasiyası
try:
    oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_DIR)
except Exception as e:
    print(f"CRITICAL: Oracle Client yüklənmədi: {e}")

# ==============================================================================
#                           EXCEL FORMATLAMA FUNKSİYASI
# ==============================================================================
def save_to_excel_styled(df, filename, sheet_name):
    """Excel faylını yaradır və peşəkar vizual formatları tətbiq edir."""
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]

            # Üslub elementləri
            bold_font = Font(bold=True, color="000000")
            center_aligned = Alignment(horizontal="center", vertical="center")
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )

            # Birinci sətiri (başlıqları) dondur
            worksheet.freeze_panes = "A2"

            for idx, col in enumerate(df.columns):
                # Başlıq xanasını formatla
                cell = worksheet.cell(row=1, column=idx+1)
                cell.font = bold_font
                cell.alignment = center_aligned
                cell.border = thin_border

                # Sütun genişliyini məzmunun uzunluğuna görə hesabla
                column_data = df[col].fillna("").astype(str)
                max_len = max(column_data.map(len).max(), len(col)) + 5

                # Sütun hərfini tap (A, B... Z, AA, AB...)
                if idx < 26:
                    col_letter = chr(65 + idx)
                else:
                    col_letter = chr(64 + idx // 26) + chr(65 + idx % 26)

                worksheet.column_dimensions[col_letter].width = min(max_len, 65)

                # Bütün məlumat xanalarına border əlavə et
                for row_idx in range(2, len(df) + 2):
                    worksheet.cell(row=row_idx, column=idx+1).border = thin_border
    except Exception as e:
        print(f"ERROR: Excel yaradılarkən xəta: {e}")

# ==============================================================================
#                             MAİL GÖNDƏRMƏ FUNKSİYASI
# ==============================================================================
def send_report_mail(report_type, file_path, target_date):
    """Hazırlanmış hesabatı TO və CC siyahılarına göndərir."""
    try:
        subject = f"Babək filialı üzrə {report_type} borc hesabatı-{target_date}"
        body = f"""Salam,

Tələbinizə uyğun olaraq, Babək filialı üzrə {target_date} tarixinədək olan ödəniş, xidmət və dərman xərclərini əks etdirən {report_type.lower()} borc hesabatını əlavə edirəm.

Hörmətlə,"""

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = SENDER_EMAIL
        msg['To'] = MAIL_TO
        if MAIL_CC:
            msg['Cc'] = ", ".join(MAIL_CC)

        msg.set_content(body)

        # Faylı mailə qoşma kimi əlavə et
        with open(file_path, 'rb') as f:
            msg.add_attachment(
                f.read(),
                maintype='application',
                subtype='octet-stream',
                filename=os.path.basename(file_path)
            )

        # BMP serverinə qoşulma və göndəriş
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=45) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASS)
            server.send_message(msg)

    except Exception as e:
        print(f"ERROR: Mail göndərilmədi: {e}")

# ==============================================================================
#                          TELEGRAM BOT İNİTİALİZASİYASI
# ==============================================================================
async def post_init(application):
    """Bot işə düşəndə menyu düyməsini Telegram-da yaradır."""
    await application.bot.set_my_commands([
        BotCommand("start", "📅 Hesabat menyusunu aç")
    ])

# ==============================================================================
#                          TƏQVİM VƏ MESAJ İDARƏETMƏSİ
# ==============================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ay seçimi düymələrini göstərir."""
    if update.effective_chat.id != ALLOWED_CHAT_ID:
        return

    keyboard = [
        [InlineKeyboardButton("Yanvar", callback_data="month_01"), InlineKeyboardButton("Fevral", callback_data="month_02"), InlineKeyboardButton("Mart", callback_data="month_03")],
        [InlineKeyboardButton("Aprel", callback_data="month_04"), InlineKeyboardButton("May", callback_data="month_05"), InlineKeyboardButton("İyun", callback_data="month_06")],
        [InlineKeyboardButton("İyul", callback_data="month_07"), InlineKeyboardButton("Avqust", callback_data="month_08"), InlineKeyboardButton("Sentyabr", callback_data="month_09")],
        [InlineKeyboardButton("Oktyabr", callback_data="month_10"), InlineKeyboardButton("Noyabr", callback_data="month_11"), InlineKeyboardButton("Dekabr", callback_data="month_12")]
    ]
    await update.message.reply_text("📅 Hesabat üçün ayı seçin:", reply_markup=InlineKeyboardMarkup(keyboard))

async def month_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Seçilən aya görə günləri hesablayır və göstərir."""
    query = update.callback_query
    await query.answer()

    month = query.data.split("_")[1]
    current_year = datetime.now().year

    if month == "02":
        is_leap = (current_year % 4 == 0 and (current_year % 100 != 0 or current_year % 400 == 0))
        days = 29 if is_leap else 28
    elif month in ["04", "06", "09", "11"]:
        days = 30
    else:
        days = 31

    keyboard = []
    current_row = []
    for day in range(1, days + 1):
        day_str = str(day).zfill(2)
        callback_data = f"date_{current_year}-{month}-{day_str}"
        current_row.append(InlineKeyboardButton(day_str, callback_data=callback_data))
        if len(current_row) == 7:
            keyboard.append(current_row)
            current_row = []
    if current_row:
        keyboard.append(current_row)

    await query.edit_message_text(
        text=f"📆 Seçilən: {month}/{current_year}. İndi günü seçin:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gün seçildikdə bazaya qoşulur və hesabatları hazırlayır."""
    query = update.callback_query
    await query.answer()

    target_date = query.data.split("_")[1]
    await query.edit_message_text(text=f"⌛ {target_date} tarixi emal edilir, xahiş olunur gözləyin...")

    def run_full_logic():
        try:
            with oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN) as conn:

                # --- AMBULATOR SQL SORĞUSU ---
                amb_sql = f"""
                SELECT
                    x.qeyd AS "Qeyd",
                    TO_CHAR(x.MuracietTarixi, 'DD.MM.YYYY') AS "Müraciət tarixi",
                    TO_CHAR(x.cixisTarixi, 'DD.MM.YYYY') AS "Bağlanma tarixi",
                    x.adisoyadi AS "Adı Soyadı",
                    x.hk_hastaturu AS "Xəstə növü",
                    x.hk_kodu AS "Kart nömrəsi",
                    CASE WHEN x.hk_HSINIFID = 1 THEN 'Ödənişli' WHEN x.hk_HSINIFID = 2 THEN 'Sığortalı' ELSE 'Digər' END AS "Xəstə statusu",
                    x.avans AS "Avans",
                    x.NEGD AS "Nağd ödəniş",
                    x.XIDMETLER AS "Xidmətlər",
                    x.DERMANLAR AS "Dərmanlar",
                    x.AVANSDAN AS "Avansdan xərclənən",
                    x.AVANS_QAYTARMA AS "Avans qaytarma",
                    x.ODENILMEYEN_X AS "Ödənilməyən xidmətlər",
                    x.ODENILMEYEN_D AS "Ödenilməyən dərmanlar",
                    (x.AVANS - x.AVANSDAN - x.AVANS_QAYTARMA) AS "Avans qalığı",
                    CASE WHEN x.hk_HSINIFID = 2 THEN (x.AVANS + x.NEGD - (x.XIDMETLER + x.DERMANLAR + x.AVANS_QAYTARMA)) ELSE NULL END AS "Sığortalı borc",
                    CASE WHEN x.hk_HSINIFID != 2 THEN (x.AVANS + x.NEGD - (x.XIDMETLER + x.DERMANLAR + x.AVANS_QAYTARMA)) ELSE NULL END AS "Borcu/Alacağı",
                    x.doktor AS "Həkim adları"
                FROM (
                    SELECT
                        t.hk_aciklama as qeyd, t.hk_adi || ' ' || t.hk_soyadi AS adisoyadi, t.hk_kodu,
                        t.hk_muracaattar AS MuracietTarixi, t.hk_hastaturu, t.hk_HSINIFID, t.Hk_Cikistarihi AS cixisTarihi,
                        COALESCE((SELECT SUM(v.TUTAR) FROM FONETHBYSADM.VZN_GIRISLER_DETAY v JOIN FONETHBYSADM.VZN_GIRISLER v1 ON v.FISID = v1.ID WHERE v1.HK_ID = t.HK_ID AND v1.TARIH <= DATE '{target_date}'), 0) AS avans,
                        COALESCE((
                            SELECT SUM(h.TUTAR) 
                            FROM FONETHBYSADM.VZN_TAHSILAT h1 
                            JOIN FONETHBYSADM.VZN_TAHSILAT_ALT h ON h.FIS_ID = h1.ID 
                            WHERE h1.HKID = t.HK_ID 
                              AND COALESCE(h.IADEDURUM, 'H') = 'H' 
                              AND NOT EXISTS (
                                  SELECT 1 
                                  FROM FONETHBYSADM.VZN_AVANSHARCAMA h2 
                                  WHERE h2.TAHSILAT_ID = h1.ID 
                                    AND h2.TAHSILAT_ID IS NOT NULL
                              ) 
                              AND h1.TARIH <= DATE '{target_date}'
                        ), 0) AS NEGD,
                        COALESCE((SELECT SUM(j.HI_MIKTAR * CASE WHEN j.HI_ODTURU = 'Sigortalı' THEN j.HI_KFIYAT ELSE j.HI_HFIYAT END) FROM FONETHBYS.H_HASTAKAYIT_ALT j WHERE j.HI_KAYITID = t.HK_ID AND j.HI_TARIH <= DATE '{target_date}'), 0) AS XIDMETLER,
                        COALESCE((SELECT ROUND(SUM(y.MIKTAR * y.FIYAT), 2) FROM FONETHBYS.STOK_CIKISDETAY y WHERE y.HASTA_HKID = t.HK_ID AND y.TARIH <= DATE '{target_date}'), 0) AS DERMANLAR,
                        COALESCE((SELECT SUM(a.TUTAR) FROM FONETHBYSADM.VZN_AVANSHARCAMA a WHERE a.HK_ID = t.HK_ID AND a.ETAR <= DATE '{target_date}'), 0) AS AVANSDAN,
                        COALESCE((SELECT SUM(yy.ODENEN_TUTAR) FROM FONETHBYSADM.VZN_ODEMELER_DETAY yy WHERE yy.ilgi_fisid = t.HK_ID AND yy.ODEME_TURU = 41 AND yy.TARIH <= DATE '{target_date}'), 0) AS AVANS_QAYTARMA,
                        COALESCE((SELECT SUM(j.HI_MIKTAR * CASE WHEN j.HI_ODTURU = 'Sigortalı' THEN j.HI_KFIYAT ELSE j.HI_HFIYAT END) FROM FONETHBYS.H_HASTAKAYIT_ALT j WHERE j.HI_KAYITID = t.HK_ID AND (j.HI_VEZNEID IS NULL OR j.HI_VEZNEID = -9) AND j.HI_TARIH <= DATE '{target_date}'), 0) AS ODENILMEYEN_X,
                        COALESCE((SELECT ROUND(SUM(y.MIKTAR * y.FIYAT), 2) FROM FONETHBYS.STOK_CIKISDETAY y WHERE y.HASTA_HKID = t.HK_ID AND (y.VEZNEID IS NULL OR y.VEZNEID = -9) AND y.TARIH <= DATE '{target_date}'), 0) AS ODENILMEYEN_D,
                        (SELECT LISTAGG(COALESCE(z.p_ad || ' ' || z.p_soyad, 'Bilinməyən'), ', ') WITHIN GROUP (ORDER BY z.p_ad, z.p_soyad) FROM FONETHBYS.H_YHSERVIS ys LEFT JOIN FONETHBYSADM.H_PERSON z ON z.p_id = ys.DOKTORID WHERE ys.HKID = t.HK_ID) AS doktor
                    FROM FONETHBYS.H_HASTAKAYIT t
                    WHERE t.hk_muracaattar >= TO_DATE('01.01.2025', 'DD.MM.YYYY') AND t.hk_muracaattar <= DATE '{target_date}' AND t.Hk_Durum = '+' AND t.hk_hastaturu='A-Ayaktan'
                ) x
                WHERE (x.avans + x.NEGD - (x.XIDMETLER + x.DERMANLAR + x.AVANS_QAYTARMA)) <> 0
                ORDER BY x.MuracietTarixi
                """

                # --- STASİONAR SQL SORĞUSU ---
                sta_sql = f"""
                SELECT
                    x.qeyd AS "Qeyd",
                    TO_CHAR(x.MuracietTarixi, 'DD.MM.YYYY') AS "Müraciət tarixi",
                    x.adisoyadi AS "Adı Soyadı",
                    x.hk_hastaturu AS "Xəstə növü",
                    x.hk_kodu AS "Kart nömrəsi",
                    CASE WHEN x.hk_HSINIFID = 1 THEN 'Ödənişli' WHEN x.hk_HSINIFID = 2 THEN 'Sığortalı' ELSE 'Digər' END AS "Xəstə statusu",
                    x.sobe AS "Yatış şöbəsi",
                    TO_CHAR(x.neql, 'DD.MM.YYYY') AS "Nəql tarixi",
                    x.avans AS "Avans",
                    x.NEGD AS "Nağd ödəniş",
                    x.XIDMETLER AS "Xidmətlər",
                    x.DERMANLAR AS "Dərmanlar",
                    x.AVANSDAN AS "Avansdan xərclənən",
                    x.AVANS_QAYTARMA AS "Avans qaytarma",
                    x.ODENILMEYEN_X AS "Ödənilməyən xidmətlər",
                    x.ODENILMEYEN_D AS "Ödenilməyən dərmanlar",
                    (x.AVANS - x.AVANSDAN - x.AVANS_QAYTARMA) AS "Avans qalığı",
                    CASE WHEN x.hk_HSINIFID = 2 THEN (x.AVANS + x.NEGD - (x.XIDMETLER + x.DERMANLAR + x.AVANS_QAYTARMA)) ELSE NULL END AS "Sığortalı borc",
                    CASE WHEN x.hk_HSINIFID != 2 THEN (x.AVANS + x.NEGD - (x.XIDMETLER + x.DERMANLAR + x.AVANS_QAYTARMA)) ELSE NULL END AS "Borcu/Alacağı",
                    x.doktor AS "Həkim adları"
                FROM (
                    SELECT
                        t.hk_aciklama AS qeyd, t.hk_adi || ' ' || t.hk_soyadi AS adisoyadi, t.hk_kodu, t.hk_muracaattar AS MuracietTarixi,
                        t.hk_hastaturu, t.hk_HSINIFID, b.nakil_tarih AS neql, W.SR_ADI AS sobe,
                        COALESCE((SELECT SUM(v.TUTAR) FROM FONETHBYSADM.VZN_GIRISLER_DETAY v JOIN FONETHBYSADM.VZN_GIRISLER v1 ON v.FISID = v1.ID WHERE v1.HK_ID = t.HK_ID AND v1.TARIH <= DATE '{target_date}'), 0) AS AVANS,
                        COALESCE((
                            SELECT SUM(h.TUTAR) 
                            FROM FONETHBYSADM.VZN_TAHSILAT h1 
                            JOIN FONETHBYSADM.VZN_TAHSILAT_ALT h ON h.FIS_ID = h1.ID 
                            WHERE h1.HKID = t.HK_ID 
                              AND COALESCE(h.IADEDURUM, 'H') = 'H' 
                              AND NOT EXISTS (
                                  SELECT 1 
                                  FROM FONETHBYSADM.VZN_AVANSHARCAMA h2 
                                  WHERE h2.TAHSILAT_ID = h1.ID 
                                    AND h2.TAHSILAT_ID IS NOT NULL
                              ) 
                              AND h1.TARIH <= DATE '{target_date}'
                        ), 0) AS NEGD,
                        COALESCE((SELECT SUM(j.HI_MIKTAR * CASE WHEN j.HI_ODTURU = 'Sigortalı' THEN j.HI_KFIYAT ELSE j.HI_HFIYAT END) FROM FONETHBYS.H_HASTAKAYIT_ALT j WHERE j.HI_KAYITID = t.HK_ID AND j.HI_TARIH <= DATE '{target_date}'), 0) AS XIDMETLER,
                        COALESCE((SELECT ROUND(SUM(y.MIKTAR * y.FIYAT), 2) FROM FONETHBYS.STOK_CIKISDETAY y WHERE y.HASTA_HKID = t.HK_ID AND y.TARIH <= DATE '{target_date}'), 0) AS DERMANLAR,
                        COALESCE((SELECT SUM(a.TUTAR) FROM FONETHBYSADM.VZN_AVANSHARCAMA a WHERE a.HK_ID = t.HK_ID AND a.ETAR <= DATE '{target_date}'), 0) AS AVANSDAN,
                        COALESCE((SELECT SUM(yy.ODENEN_TUTAR) FROM FONETHBYSADM.VZN_ODEMELER_DETAY yy WHERE yy.ILGI_FISID = t.HK_ID AND yy.ODEME_TURU = 41 AND yy.TARIH <= DATE '{target_date}'), 0) AS AVANS_QAYTARMA,
                        COALESCE((SELECT SUM(j.HI_MIKTAR * CASE WHEN j.HI_ODTURU = 'Sigortalı' THEN j.HI_KFIYAT ELSE j.HI_HFIYAT END) FROM FONETHBYS.H_HASTAKAYIT_ALT j WHERE j.HI_KAYITID = t.HK_ID AND j.HI_TARIH <= DATE '{target_date}' AND (j.HI_VEZNEID IS NULL OR j.HI_VEZNEID = -9)), 0) AS ODENILMEYEN_X,
                        COALESCE((SELECT ROUND(SUM(y.MIKTAR * y.FIYAT), 2) FROM FONETHBYS.STOK_CIKISDETAY y WHERE y.HASTA_HKID = t.HK_ID AND y.TARIH <= DATE '{target_date}' AND (y.VEZNEID IS NULL OR y.VEZNEID = -9)), 0) AS ODENILMEYEN_D,
                        (SELECT LISTAGG(COALESCE(z.p_ad || ' ' || z.p_soyad, 'Bilinməyən'), ', ') WITHIN GROUP (ORDER BY z.p_ad, z.p_soyad) FROM FONETHBYS.H_YHSERVIS ys LEFT JOIN FONETHBYSADM.H_PERSON z ON z.p_id = ys.DOKTORID WHERE ys.HKID = t.HK_ID) AS doktor
                    FROM FONETHBYS.H_HASTAKAYIT t
                    INNER JOIN FONETHBYS.H_YHNAKIL b ON b.HKID = t.HK_ID
                    LEFT JOIN FONETHBYSADM.H_SERVIS W ON W.SR_ID = b.GELIS_SERVISID
                    WHERE t.hk_muracaattar >= TO_DATE('01.01.2025', 'DD.MM.YYYY') AND t.hk_muracaattar <= DATE '{target_date}' AND t.hk_durum = '+' AND t.hk_hastaturu = 'Y-Yatan'
                ) x
                WHERE (x.AVANS + x.NEGD - (x.XIDMETLER + x.DERMANLAR + x.AVANS_QAYTARMA)) <> 0
                ORDER BY x.MuracietTarixi
                """

                # Hesabat 1: Ambulator
                df_amb = pd.read_sql(amb_sql, conn)
                name_amb = f"Babək filialı üzrə ambulator borc hesabatı-{target_date}.xlsx"
                save_to_excel_styled(df_amb, name_amb, "Ambulator")
                send_report_mail("Ambulator", name_amb, target_date)
                if os.path.exists(name_amb): os.remove(name_amb)

                # Hesabat 2: Stasionar
                df_sta = pd.read_sql(sta_sql, conn)
                name_sta = f"Babək filialı üzrə stasionar borc hesabatı-{target_date}.xlsx"
                save_to_excel_styled(df_sta, name_sta, "Stasionar")
                send_report_mail("Stasionar", name_sta, target_date)
                if os.path.exists(name_sta): os.remove(name_sta)

                return f"✅ {target_date} üçün hesabatlar TO və CC siyahılarına göndərildi."

        except Exception as e:
            return f"❌ CRITICAL ERROR: {str(e)}"

    # Botun cavabdehliyini itirməməsi üçün ağır işləri executor-da işlədirik
    loop = asyncio.get_event_loop()
    final_result = await loop.run_in_executor(None, run_full_logic)
    await context.bot.send_message(chat_id=ALLOWED_CHAT_ID, text=final_result)

# ==============================================================================
#                                BOTU İŞƏ SALMAQ
# ==============================================================================
if __name__ == '__main__':
    # Ubuntu-da timeout limitləri daha uzun tutulmalıdır
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).connect_timeout(60).read_timeout(60).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(month_handler, pattern="^month_"))
    app.add_handler(CallbackQueryHandler(date_handler, pattern="^date_"))

    print("🚀 Babək Filialı Hesabat Botu aktivdir (Ubuntu)...")
    app.run_polling(poll_interval=1.0, timeout=45)
