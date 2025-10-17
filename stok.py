# -*- coding: utf-8 -*-
"""
Stok çıxış statistikası (Oracle THICK MODE, Instant Client)
- Ay-ay icra: hər ay ayrıca Excel sheet (Yanvar, Fevral, ...).
- Sütunlar: DEPO, Xeste sayi, Mebleg, Alis meblegi, Paket daxili edilmis mebleg.
- Rəngli header, auto-fit sütunlar, filter, freeze panes, CƏM sətiri.
- Pul sütunlarında data bar vizualı.
"""

import os
import warnings
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
import oracledb


# ================== PARAMETRLƏR ==================
# Oracle giriş (istəyə görə ENV istifadə edə bilərsən)
DB_USER = os.getenv("ORA_USER", "NURAN")                         # <-- DƏYİŞ
DB_PASS = os.getenv("ORA_PASS", "Nuran..2024!!")                 # <-- DƏYİŞ
DB_DSN  = os.getenv("ORA_DSN",  "172.18.79.23:1521/FONETAZ")     # <-- DƏYİŞ

# Instant Client qovluğu (thick mode üçün şərtdir)
INSTANT_CLIENT_DIR =  r"C:\instant\instantclient_23_9"  # <-- YOLU YOXLA

# Tarix aralığı (bitis EXCLUSIVE). Məsələn: 2025-08-01 → 2025-11-01 (Avq–Okt)
START_DATE = date(2025, 1, 1)
END_DATE   = date(2025, 11, 1)

# Filtrlər
ODEME_DURUM = "*Ödenmis"
FORMTYPE_LIST = (2,)   # yalnız 2; 2,3,10 da lazım olsa: (2,3,10)

# Çıxış faylı
OUTPUT_XLSX = "Stok_Cixis_Stat_Depoya_Gore_Aylıq.xlsx"


# ================== SQL ==================
SQL = f"""
SELECT
  TRUNC(T.TARIH,'MM')            AS AY,
  T.DEPOADI                      AS DEPO,
  COUNT(DISTINCT T.HASTA_HKID)   AS "Xeste sayi",
  SUM(T.SNLTUTAR)                AS "Mebleg",
  SUM(T.SNLALISTUTAR)            AS "Alis meblegi",
  SUM(T.TUTAR_PKTSIZ)            AS "Paket daxili edilmis mebleg"
FROM fonethbys.V_STOK_LIST_CIKISFISDETAY T
WHERE T.FORMTYPE IN ({",".join(str(x) for x in FORMTYPE_LIST)})
  AND T.TARIH >= :p_start
  AND T.TARIH <  :p_end
  AND T.ODEMEDURUM = :p_odemestatus
GROUP BY TRUNC(T.TARIH,'MM'), T.DEPOADI
ORDER BY TRUNC(T.TARIH,'MM'), T.DEPOADI
"""


# ================== KÖMƏKÇİ FUNKSİYALAR ==================
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
        max_len = max([len(col)] + series.map(len).tolist()) if len(series) else len(col)
        ws.set_column(start_col + i, start_col + i, max_len + extra)


# ================== ƏSAS FUNKSİYA ==================
def main():
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

    # THICK MODE
    try:
        oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_DIR)
    except Exception as e:
        raise SystemExit(f"Instant Client init xətası. Yol düzgünmü?\n{e}")

    # DB bağlantısı
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
    except oracledb.Error as e:
        raise SystemExit(f"DB bağlantı xətası: {e}")

    with conn:
        # Excel writer (NaN/INF üçün təhlükəsizlik)
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
                # NaN/INF-ləri sıfırla
                df = df.fillna(0)

                # “AY” sütununu sheet adı olduğu üçün çıxar
                if "AY" in df.columns:
                    df = df.drop(columns=["AY"])

                # Sheet-ə yaz (header-ı özümüz boyayacağıq deyə startrow=2)
                df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=2)
                ws = writer.sheets[sheet_name]

                # Başlıq mətni
                ws.write(0, 0, f"{sheet_name} ayı stok çıxış statistikası", title_fmt)
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
                        if name in ("Xeste sayi",):
                            ws.write(3 + r, c, val, int_fmt)
                        elif name in ("Mebleg", "Alis meblegi", "Paket daxili edilmis mebleg"):
                            ws.write(3 + r, c, val, money_fmt)
                        else:
                            ws.write(3 + r, c, val, text_fmt)

                # CƏM sətiri
                total_row = 3 + nrows
                if nrows > 0:
                    ws.write(total_row, 0, "CƏM", total_int_fmt)
                    col_idx = {c: i for i, c in enumerate(df.columns)}

                    # Depo üçün boş/label görünüşü
                    if "DEPO" in col_idx:
                        ws.write(total_row, col_idx["DEPO"], "", total_int_fmt)

                    # SUM formulaları
                    if "Xeste sayi" in col_idx:
                        c = col_idx["Xeste sayi"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_int_fmt)
                    if "Mebleg" in col_idx:
                        c = col_idx["Mebleg"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_fmt)
                    if "Alis meblegi" in col_idx:
                        c = col_idx["Alis meblegi"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_fmt)
                    if "Paket daxili edilmis mebleg" in col_idx:
                        c = col_idx["Paket daxili edilmis mebleg"]
                        ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_fmt)

                # Auto-fit, filter, freeze
                autofit_columns(writer, sheet_name, df)
                ws.autofilter(2, 0, max(2 + nrows, 3), len(df.columns) - 1)
                ws.freeze_panes(3, 0)

                # Data bars (vizual)
                if nrows > 1:
                    for colname in ("Mebleg", "Alis meblegi", "Paket daxili edilmis mebleg"):
                        if colname in df.columns:
                            c = df.columns.get_loc(colname)
                            ws.conditional_format(3, c, 2 + nrows, c, {"type": "data_bar"})

                # Növbəti ay
                cur_start = cur_end

    print(f"OK: {OUTPUT_XLSX} yaradıldı.")


# ================== GİRİŞ NÖQTƏSİ ==================
if __name__ == "__main__":
    main()
