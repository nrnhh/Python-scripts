# -*- coding: utf-8 -*-
"""
Aylıq xidmət statistikası (Oracle THICK MODE, Instant Client)
- Hər ay üçün ayrı Excel sheet (Yanvar, Fevral, ..., Sentyabr).
- Rəngli başlıq, auto-fit sütunlar, filter, freeze panes.
- CƏM sətiri və "Mebleg" üçün data bar vizualı.
"""

import os
import warnings
from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
import oracledb


# =============== PARAMETRLƏR ===============
# Oracle giriş (istəsən env-dən oxu: ORA_USER/ORA_PASS/ORA_DSN)
DB_USER = os.getenv("ORA_USER", "NURAN")                         # <-- DƏYİŞ
DB_PASS = os.getenv("ORA_PASS", "Nuran..2024!!")                 # <-- DƏYİŞ
DB_DSN  = os.getenv("ORA_DSN",  "172.18.79.23:1521/FONETAZ")     # <-- DƏYİŞ

# Instant Client qovluğu (səndə artıq var)
INSTANT_CLIENT_DIR =  r"C:\instant\instantclient_23_9" # <-- YOLU YOXLA

# Tarix aralığı (bitis EXCLUSIVE). Burada 2025 Yanvar–Sentyabr.
START_DATE = date(2025, 1, 1)
END_DATE   = date(2025, 10, 1)

# Ödəniş statusu filtri
ODEME_DURUM = "*Ödenmis"

# Çıxış Excel faylı
OUTPUT_XLSX = "Aylıq_Xidmət_Statistikası_2025_Jan-Sep.xlsx"


# =============== SQL ===============
SQL = """
SELECT
  TRUNC(T.HI_TARIH,'MM')               AS AY,
  T.SR_ID                               AS ID,
  T.SR_ADI                               AS ADI,
  COUNT(DISTINCT T.HK_ID)               AS "Xeste sayi",
  SUM(T.HI_MIKTAR)                      AS "Xidmet sayi",
  SUM(T.HI_HTUTAR)                      AS "Mebleg"
FROM fonethbys.V_IST_GENEL_HIZMET T
WHERE T.HI_TARIH >= :p_start
  AND T.HI_TARIH <  :p_end
  AND T.ODEMEDURUM = :p_odemestatus
GROUP BY TRUNC(T.HI_TARIH,'MM'), T.SR_ID, T.SR_ADI
ORDER BY TRUNC(T.HI_TARIH,'MM'), T.SR_ADI
"""


# =============== KÖMƏKÇİ FUNKSİYALAR ===============
def xl_col(idx_zero_based: int) -> str:
    """0-based sütun indeksini Excel hərfinə çevir (0->A, 25->Z, 26->AA, ...)."""
    idx = idx_zero_based
    letters = ""
    while True:
        idx, rem = divmod(idx, 26)
        letters = chr(65 + rem) + letters
        if idx == 0:
            break
        idx -= 1
    return letters


def autofit_columns(writer, sheet_name, df, start_col=0, extra=2):
    """Sütun genişliklərini başlıq və data uzunluğuna görə auto-fit et."""
    ws = writer.sheets[sheet_name]
    for i, col in enumerate(df.columns):
        series = df[col].astype(str)
        max_len = max([len(col)] + series.map(len).tolist())
        ws.set_column(start_col + i, start_col + i, max_len + extra)


# =============== ƏSAS FUNKSİYA ===============
def main():
    # Pandas xəbərdarlığını susdur (istəyə bağlı)
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

    # ---- THICK MODE aktivləşdir ----
    try:
        oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_DIR)
    except Exception as e:
        raise SystemExit(f"Instant Client init xətası. Yol düzgünmü?\n{e}")

    # ---- DB bağlantısı ----
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
    except oracledb.Error as e:
        raise SystemExit(f"DB bağlantı xətası: {e}")

    with conn:
        # Excel writer (NaN/INF üçün təhlükəsizlik opsiyası)
        with pd.ExcelWriter(
            OUTPUT_XLSX,
            engine="xlsxwriter",
            engine_kwargs={"options": {"nan_inf_to_errors": True}}
        ) as writer:
            wb = writer.book

            # Formatlar
            header_fmt = wb.add_format({
                "bold": True, "font_size": 11, "bg_color": "#1F4E78",
                "font_color": "white", "align": "center", "valign": "vcenter", "border": 1
            })
            int_fmt   = wb.add_format({"num_format": "#,##0", "border": 1})
            money_fmt = wb.add_format({"num_format": "#,##0.00", "border": 1})
            text_fmt  = wb.add_format({"border": 1})
            total_fmt = wb.add_format({
                "bold": True, "bg_color": "#DEEAF6", "border": 1, "num_format": "#,##0.00"
            })
            total_int_fmt = wb.add_format({
                "bold": True, "bg_color": "#DEEAF6", "border": 1, "num_format": "#,##0"
            })
            title_fmt = wb.add_format({"bold": True, "font_size": 14})

            # Ay adları (AZ)
            az_map = {
                1: "Yanvar", 2: "Fevral", 3: "Mart", 4: "Aprel", 5: "May", 6: "İyun",
                7: "İyul", 8: "Avqust", 9: "Sentyabr", 10: "Oktyabr", 11: "Noyabr", 12: "Dekabr"
            }

            cur_start = START_DATE
            while cur_start < END_DATE:
                cur_end = cur_start + relativedelta(months=1)
                sheet_name = az_map[cur_start.month]

                # Sorğunu aylıq interval üçün işlə
                df = pd.read_sql(
                    SQL, conn,
                    params={"p_start": cur_start, "p_end": cur_end, "p_odemestatus": ODEME_DURUM}
                )

                # NaN/INF-ləri sıfırla (xlsxwriter error-unun qarşısı)
                df = df.fillna(0)

                # “AY” sütununu göstərmək istəmiriksə, atırıq (sheet adı var)
                if "AY" in df.columns:
                    df = df.drop(columns=["AY"])

                # Sheet-ə yaz (header-ı özümüz boyayacağıq deyə startrow=2)
                df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=2)
                ws = writer.sheets[sheet_name]

                # Başlıq mətni
                ws.write(0, 0, f"{sheet_name} ayı xidmət statistikası", title_fmt)
                ws.write(1, 0, f"Tarix aralığı: {cur_start.strftime('%d.%m.%Y')} – {(cur_end - relativedelta(days=1)).strftime('%d.%m.%Y')}")

                # Header formatı
                for col_idx, col_name in enumerate(df.columns):
                    ws.write(2, col_idx, col_name, header_fmt)

                # Sətir hündürlüyü və hüceyrə formatları
                nrows = len(df)
                for r in range(nrows):
                    ws.set_row(3 + r, 18)
                    for c, name in enumerate(df.columns):
                        val = df.iloc[r, c]
                        if name in ("ID", "Xeste sayi", "Xidmet sayi"):
                            ws.write(3 + r, c, val, int_fmt)
                        elif name == "Mebleg":
                            ws.write(3 + r, c, val, money_fmt)
                        else:
                            ws.write(3 + r, c, val, text_fmt)

                # CƏM sətiri
                total_row = 3 + nrows
                if nrows > 0:
                    ws.write(total_row, 0, "CƏM", total_int_fmt)

                    col_idx = {c: i for i, c in enumerate(df.columns)}

                    # Boş hüceyrələr (vizual üçün)
                    if "ID" in col_idx:
                        ws.write(total_row, col_idx["ID"], "", total_int_fmt)
                    if "ADI" in col_idx:
                        ws.write(total_row, col_idx["ADI"], "", total_int_fmt)

                    # SUM formulaları (A4:Ax kimi)
                    if "Xeste sayi" in col_idx:
                        c = col_idx["Xeste sayi"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_int_fmt)
                    if "Xidmet sayi" in col_idx:
                        c = col_idx["Xidmet sayi"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_int_fmt)
                    if "Mebleg" in col_idx:
                        c = col_idx["Mebleg"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_fmt)

                # Auto-fit, filter, freeze
                autofit_columns(writer, sheet_name, df)
                if nrows >= 0:
                    ws.autofilter(2, 0, 2 + max(nrows, 1), len(df.columns) - 1)
                ws.freeze_panes(3, 0)

                # “Mebleg” üçün data bar (vizual)
                if "Mebleg" in df.columns and nrows > 1:
                    c = df.columns.get_loc("Mebleg")
                    ws.conditional_format(3, c, 2 + nrows, c, {"type": "data_bar"})

                # Növbəti ay
                cur_start = cur_end

    print(f"OK: {OUTPUT_XLSX} yaradıldı.")


# =============== GİRİŞ NÖQTƏSİ ===============
if __name__ == "__main__":
    main()
