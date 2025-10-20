# -*- coding: utf-8 -*-
"""
Aylıq Dərman + Xidmət hesabatı (Oracle THICK MODE)

- Hər ay üçün 2 sheet: "<Ay> - Dərman", "<Ay> - Xidmət"
- "Aylıq Özet": Ay səviyyəsində
    * Dərman Məbləğ (CƏM), Dərman EDV-siz (CƏM), Dərman İcbari
    * Xidmət EDV-siz (Ödənişli), Xidmət EDV-siz (Sığortalı), Xidmət İcbari (CƏM)
- "Aylıq Özet (Depo - Dərman)": Ay + Depo səviyyəsində
    * Dərman EDV-siz (CƏM), Dərman İcbari
- "Aylıq Özet (Şöbə - Xidmət)": Ay + Şöbə səviyyəsində
    * Xidmət EDV-siz (Ödənişli), Xidmət EDV-siz (Sığortalı), Xidmət İcbari (CƏM)
- "Şöbə Aylıq Cəm (Xidmət)": Pivot — ay sütunları + MoM Δ + Yekun (azalan sort)
- "Depo Aylıq Cəm (Dərman)": Pivot — ay sütunları + MoM Δ + Yekun (azalan sort)

Qeyd: EDV = 18%. Xidmət üçün "EDV-siz (Sığortalı)" = Təşkilat məbləği / 1.18 (İcbariyə EDV tətbiq olunmur).
"""

import os, warnings
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
import oracledb

# ================== PARAMETRLƏR ==================
DB_USER = os.getenv("ORA_USER", "NURAN")                         # <-- DƏYİŞ
DB_PASS = os.getenv("ORA_PASS", "Nuran..2024!!")                 # <-- DƏYİŞ
DB_DSN  = os.getenv("ORA_DSN",  "172.18.79.23:1521/FONETAZ")     # <-- DƏYİŞ

INSTANT_CLIENT_DIR = r"C:\instant\instantclient_23_9"  # <-- DƏYİŞ/YOXLA

# Tarix aralığı (bitis EXCLUSIVE). Nümunə: Avq–Oktyabr 2025.
START_DATE = date(2025, 1, 1)
END_DATE   = date(2025, 11, 1)

# Çıxış faylı (əgər mövcuddursa _1, _2 ... artırılacaq)
BASE_XLSX  = "Aylıq_Derman_Xidmet_Hesabatı.xlsx"

VAT_RATE = 0.18  # 1.18 üçün

# ================== SQL-lər ==================
# DƏRMAN — ÜMUMİ MƏBLƏĞ (ödənişli + icbari), EDV-siz, İcbari
SQL_DERMAN = """
SELECT
  TRUNC(T.TARIH,'MM') AS AY,
  T.DEPOADI           AS DEPO,
  COUNT(DISTINCT T.HASTA_HKID) AS XESTE_SAYI,
  SUM(NVL(T.SNLTUTAR,0))      AS MEBLEG,
  SUM(NVL(T.SNLTUTAR,0))/1.18 AS EDVSIZ,
  SUM(CASE WHEN S.HK_KURUMID = 812 THEN NVL(T.SNLTUTAR,0) ELSE 0 END) AS MEBLEG_ICBARI,
  SUM(NVL(T.SNLALISTUTAR,0)) AS ALIS_MEBLEGI
FROM fonethbys.V_STOK_LIST_CIKISFISDETAY T
LEFT JOIN fonethbys.H_HASTAKAYIT S ON S.HK_ID = T.HASTA_HKID
WHERE T.FORMTYPE = 2
  AND T.TARIH >= :p_start
  AND T.TARIH <  :p_end
GROUP BY TRUNC(T.TARIH,'MM'), T.DEPOADI
ORDER BY TRUNC(T.TARIH,'MM'), T.DEPOADI
"""

# XİDMƏT — ödənişli, təşkilat, icbari ayrı; EDV-sizlər artıq /1.18 ilə
SQL_XIDMET = """
SELECT
  TRUNC(T.HI_TARIH,'MM') AS AY,
  T.SR_ID                AS ID,
  T.SR_ADI               AS ADI,
  COUNT(DISTINCT T.HK_ID) AS XESTE_SAYI,
  SUM(NVL(T.HI_MIKTAR,0)) AS XIDMET_SAYI,

  -- Xəstə (ödənişli)
  SUM(CASE WHEN T.HK_KURUMID <> 812 THEN NVL(T.HI_HTUTAR,0) ELSE 0 END)      AS ODE_MEBLEG,
  SUM(CASE WHEN T.HK_KURUMID <> 812 THEN NVL(T.HI_HTUTAR,0) ELSE 0 END)/1.18 AS ODE_EDVSIZ,

  -- Təşkilat (sığortalı)
  SUM(CASE WHEN T.HK_KURUMID <> 812 THEN NVL(T.HI_KTUTAR,0) ELSE 0 END)      AS TESK_MEBLEG,
  SUM(CASE WHEN T.HK_KURUMID <> 812 THEN NVL(T.HI_KTUTAR,0) ELSE 0 END)/1.18 AS TESK_EDVSIZ,

  -- İcbari (brüt)
  SUM(CASE WHEN T.HK_KURUMID = 812 THEN NVL(T.HI_KTUTAR,0) + NVL(T.HI_HTUTAR,0) ELSE 0 END) AS ICBARI_TOPLAM
FROM fonethbys.V_IST_GENEL_HIZMET T
WHERE T.HI_TARIH >= :p_start
  AND T.HI_TARIH <  :p_end
GROUP BY TRUNC(T.HI_TARIH,'MM'), T.SR_ID, T.SR_ADI
ORDER BY TRUNC(T.HI_TARIH,'MM'), T.SR_ADI, T.SR_ID
"""

# Başlıqlar
PRETTY_DERMAN = {
    "DEPO": "Depo",
    "XESTE_SAYI": "Xəstə sayı",
    "MEBLEG": "Məbləğ",
    "EDVSIZ": "EDV-siz",
    "MEBLEG_ICBARI": "İcbari məbləğ",
    "ALIS_MEBLEGI": "Alış məbləği",
}
PRETTY_XIDMET = {
    "ID": "ID",
    "ADI": "Şöbə adı",
    "XESTE_SAYI": "Xəstə sayı",
    "XIDMET_SAYI": "Xidmət sayı",
    "ODE_MEBLEG": "Ödənişli məbləğ",
    "ODE_EDVSIZ": "EDV-siz ödənişli",
    "TESK_MEBLEG": "Təşkilat məbləği",
    "TESK_EDVSIZ": "EDV-siz təşkilat",
    "ICBARI_TOPLAM": "İcbari toplam",
}

AZ_MONTH = {1:"Yanvar",2:"Fevral",3:"Mart",4:"Aprel",5:"May",6:"İyun",7:"İyul",8:"Avqust",9:"Sentyabr",10:"Oktyabr",11:"Noyabr",12:"Dekabr"}

# ================== Köməkçilər ==================
def xl_col(idx0: int) -> str:
    n, s = idx0, ""
    while True:
        n, r = divmod(n, 26)
        s = chr(65 + r) + s
        if n == 0: break
        n -= 1
    return s

def autofit_columns(writer, sheet, df, extra=2):
    ws = writer.sheets[sheet]
    for i, c in enumerate(df.columns):
        ser = df[c].astype(str)
        width = max([len(c)] + ser.map(len).tolist()) if len(ser) else len(c)
        ws.set_column(i, i, width + extra)

def unique_filename(base: str) -> str:
    root, ext = os.path.splitext(base)
    k = 1
    name = base
    while os.path.exists(name):
        name = f"{root}_{k}{ext}"
        k += 1
    return name

# ================== Əsas ==================
def main():
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
    oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_DIR)
    conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)

    out_xlsx = unique_filename(BASE_XLSX)

    summary_rows = []          # Ay səviyyəsi — Dərman + Xidmət
    depo_summary_rows = []     # Ay + Depo — Dərman (özet səhifə üçün)
    sobe_summary_rows = []     # Ay + Şöbə — Xidmət (özet səhifə üçün)
    sobe_monthly_rows = []     # Pivot: Şöbə Aylıq Cəm (Xidmət)
    depo_monthly_rows = []     # Pivot: Depo Aylıq Cəm (Dərman)

    with conn:
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter",
                            engine_kwargs={"options": {"nan_inf_to_errors": True}}) as writer:
            wb = writer.book

            # ---- Formatlar ----
            header_fmt = wb.add_format({"bold": True, "font_size": 11, "bg_color": "#1F4E78",
                                        "font_color": "white", "align": "center", "valign": "vcenter", "border": 1})
            int_fmt   = wb.add_format({"num_format": "#,##0", "border": 1})
            money_fmt = wb.add_format({"num_format": "#,##0.00", "border": 1})
            text_fmt  = wb.add_format({"border": 1})
            total_fmt = wb.add_format({"bold": True, "bg_color": "#DEEAF6", "border": 1, "num_format": "#,##0.00"})
            total_int_fmt = wb.add_format({"bold": True, "bg_color": "#DEEAF6", "border": 1, "num_format": "#,##0"})
            title_fmt = wb.add_format({"bold": True, "font_size": 14})

            # ================== 1) ÖNƏ ÇƏKİLƏCƏK SHEETLƏRİ əvvəl YARAT (boş yer tutucu) ==================
            sheet_pivot_sobe = "Şöbə Aylıq Cəm (Xidmət)"
            sheet_pivot_depo = "Depo Aylıq Cəm (Dərman)"
            sheet_note       = "Qeyd"

            ws_pivot_sobe = wb.add_worksheet(sheet_pivot_sobe); ws_pivot_sobe.set_tab_color("#305496")
            ws_pivot_depo = wb.add_worksheet(sheet_pivot_depo); ws_pivot_depo.set_tab_color("#305496")
            ws_note       = wb.add_worksheet(sheet_note);       ws_note.set_tab_color("#C00000")

            # pandas writer xəritəsinə əlavə et (sonradan yazmaq üçün)
            writer.sheets[sheet_pivot_sobe] = ws_pivot_sobe
            writer.sheets[sheet_pivot_depo] = ws_pivot_depo
            writer.sheets[sheet_note]       = ws_note

            # ================== 2) MƏLUMATI TOPLA və AYLIQ SHEETLƏRİ YAZ ==================
            cur = START_DATE
            months_order = []
            while cur < END_DATE:
                nxt = cur + relativedelta(months=1)
                ay_ad = AZ_MONTH[cur.month]
                months_order.append(ay_ad)

                # ---- DƏRMAN ----
                derman_df = pd.read_sql(SQL_DERMAN, conn, params={"p_start": cur, "p_end": nxt}).fillna(0)
                if "AY" in derman_df.columns: derman_df = derman_df.drop(columns=["AY"])
                derman_df.rename(columns=PRETTY_DERMAN, inplace=True)

                sheet_der = f"{ay_ad} - Dərman"
                derman_df.to_excel(writer, sheet_name=sheet_der, index=False, startrow=2)
                ws = writer.sheets[sheet_der]
                ws.write(0, 0, f"{ay_ad} ayı — Dərmanlar", title_fmt)
                ws.write(1, 0, f"Tarix aralığı: {cur.strftime('%d.%m.%Y')} – {(nxt - relativedelta(days=1)).strftime('%d.%m.%Y')}")
                for i, col in enumerate(derman_df.columns): ws.write(2, i, col, header_fmt)

                nrows = len(derman_df)
                for r in range(nrows):
                    ws.set_row(3 + r, 18)
                    for c, name in enumerate(derman_df.columns):
                        val = derman_df.iloc[r, c]
                        if name == "Xəstə sayı":
                            ws.write(3 + r, c, val, int_fmt)
                        elif name in ("Məbləğ","EDV-siz","İcbari məbləğ","Alış məbləği"):
                            ws.write(3 + r, c, val, money_fmt)
                        else:
                            ws.write(3 + r, c, val, text_fmt)

                total_row = 3 + nrows
                if nrows > 0:
                    ws.write(total_row, 0, "CƏM", total_int_fmt)
                    idx = {c: i for i, c in enumerate(derman_df.columns)}
                    if "Depo" in idx: ws.write(total_row, idx["Depo"], "", total_int_fmt)
                    if "Xəstə sayı" in idx:
                        c = idx["Xəstə sayı"]; ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_int_fmt)
                    for k in ("Məbləğ","EDV-siz","İcbari məbləğ","Alış məbləği"):
                        if k in idx:
                            c = idx[k]; ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_fmt)

                autofit_columns(writer, sheet_der, derman_df)
                ws.autofilter(2, 0, max(2 + nrows, 3), len(derman_df.columns) - 1)
                ws.freeze_panes(3, 0)

                # ---- XİDMƏT ----
                xidmet_df = pd.read_sql(SQL_XIDMET, conn, params={"p_start": cur, "p_end": nxt}).fillna(0)
                if "AY" in xidmet_df.columns: xidmet_df = xidmet_df.drop(columns=["AY"])
                xidmet_df.rename(columns=PRETTY_XIDMET, inplace=True)

                sheet_xid = f"{ay_ad} - Xidmət"
                xidmet_df.to_excel(writer, sheet_name=sheet_xid, index=False, startrow=2)
                ws2 = writer.sheets[sheet_xid]
                ws2.write(0, 0, f"{ay_ad} ayı — Xidmətlər", title_fmt)
                ws2.write(1, 0, f"Tarix aralığı: {cur.strftime('%d.%m.%Y')} – {(nxt - relativedelta(days=1)).strftime('%d.%m.%Y')}")
                for i, col in enumerate(xidmet_df.columns): ws2.write(2, i, col, header_fmt)

                nrows2 = len(xidmet_df)
                for r in range(nrows2):
                    ws2.set_row(3 + r, 18)
                    for c, name in enumerate(xidmet_df.columns):
                        val = xidmet_df.iloc[r, c]
                        if name in ("Xəstə sayı","Xidmət sayı"):
                            ws2.write(3 + r, c, val, int_fmt)
                        elif name in ("Ödənişli məbləğ","EDV-siz ödənişli","Təşkilat məbləği","EDV-siz təşkilat","İcbari toplam"):
                            ws2.write(3 + r, c, val, money_fmt)
                        else:
                            ws2.write(3 + r, c, val, text_fmt)

                total_row2 = 3 + nrows2
                if nrows2 > 0:
                    ws2.write(total_row2, 0, "CƏM", total_int_fmt)
                    idx2 = {c: i for i, c in enumerate(xidmet_df.columns)}
                    for k in ("ID","Şöbə adı"):
                        if k in idx2: ws2.write(total_row2, idx2[k], "", total_int_fmt)
                    if "Xəstə sayı" in idx2:
                        c = idx2["Xəstə sayı"]; ws2.write_formula(total_row2, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows2})", total_int_fmt)
                    if "Xidmət sayı" in idx2:
                        c = idx2["Xidmət sayı"]; ws2.write_formula(total_row2, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows2})", total_int_fmt)
                    for k in ("Ödənişli məbləğ","EDV-siz ödənişli","Təşkilat məbləği","EDV-siz təşkilat","İcbari toplam"):
                        if k in idx2:
                            c = idx2[k]; ws2.write_formula(total_row2, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows2})", total_fmt)

                autofit_columns(writer, sheet_xid, xidmet_df)
                ws2.autofilter(2, 0, max(2 + nrows2, 3), len(xidmet_df.columns) - 1)
                ws2.freeze_panes(3, 0)

                # ---- Aylıq Özet dəyərləri (Ay səviyyəsi) ----
                derman_mebleg_cem = float(derman_df["Məbləğ"].sum()) if "Məbləğ" in derman_df.columns else 0.0
                derman_edvsiz_cem = float(derman_df["EDV-siz"].sum()) if "EDV-siz" in derman_df.columns else 0.0
                derman_icbari     = float(derman_df["İcbari məbləğ"].sum()) if "İcbari məbləğ" in derman_df.columns else 0.0

                xid_icbari_toplam    = float(xidmet_df["İcbari toplam"].sum()) if "İcbari toplam" in xidmet_df.columns else 0.0
                xid_edvsiz_sigortali = float(xidmet_df["EDV-siz təşkilat"].sum()) if "EDV-siz təşkilat" in xidmet_df.columns else 0.0
                xid_edvsiz_odenisli  = float(xidmet_df["EDV-siz ödənişli"].sum()) if "EDV-siz ödənişli" in xidmet_df.columns else 0.0

                summary_rows.append({
                    "Ay": ay_ad,
                    "Dərman Məbləğ (CƏM)": derman_mebleg_cem,
                    "Dərman EDV-siz (CƏM)": derman_edvsiz_cem,
                    "Dərman İcbari": derman_icbari,
                    "Xidmət EDV-siz (Ödənişli)": xid_edvsiz_odenisli,
                    "Xidmət EDV-siz (Sığortalı)": xid_edvsiz_sigortali,
                    "Xidmət İcbari (CƏM)": xid_icbari_toplam,
                })

                # ---- Özetlər üçün qruplar ----
                if not derman_df.empty:
                    # Aylıq Özet (Depo - Dərman)
                    dgrp = derman_df.groupby("Depo", as_index=False).agg({
                        "EDV-siz": "sum",
                        "İcbari məbləğ": "sum"
                    }).rename(columns={"EDV-siz": "Dərman EDV-siz (CƏM)", "İcbari məbləğ": "Dərman İcbari"})
                    dgrp.insert(0, "Ay", ay_ad)
                    depo_summary_rows.append(dgrp)

                    # Depo Aylıq Cəm (Dərman) — cəm = EDV-siz + İcbari
                    dgrp2 = derman_df.groupby("Depo", as_index=False).agg({
                        "EDV-siz": "sum",
                        "İcbari məbləğ": "sum"
                    })
                    dgrp2["Cəm"] = dgrp2["EDV-siz"] + dgrp2["İcbari məbləğ"]
                    dgrp2 = dgrp2[["Depo", "Cəm"]]
                    dgrp2.insert(0, "Ay", ay_ad)
                    depo_monthly_rows.append(dgrp2)

                if not xidmet_df.empty:
                    # Şöbə: Ödənişli EDV-siz + Sığortalı EDV-siz + İcbari (brüt)
                    grp = xidmet_df.groupby("Şöbə adı", as_index=False).agg({
                        "EDV-siz ödənişli": "sum",
                        "EDV-siz təşkilat": "sum",
                        "İcbari toplam": "sum",
                    }).rename(columns={
                        "EDV-siz ödənişli": "Xidmət EDV-siz (Ödənişli)",
                        "EDV-siz təşkilat": "Xidmət EDV-siz (Sığortalı)",
                        "İcbari toplam":    "Xidmət İcbari (CƏM)",
                    })
                    sgrp = grp[[
                        "Şöbə adı",
                        "Xidmət EDV-siz (Ödənişli)",
                        "Xidmət EDV-siz (Sığortalı)",
                        "Xidmət İcbari (CƏM)",
                    ]].copy()
                    sgrp.insert(0, "Ay", ay_ad)
                    sobe_summary_rows.append(sgrp)

                    tmp = grp.copy()
                    tmp["Cəm"] = (
                        tmp["Xidmət EDV-siz (Ödənişli)"] +
                        tmp["Xidmət EDV-siz (Sığortalı)"] +
                        tmp["Xidmət İcbari (CƏM)"]
                    )
                    agg = tmp[["Şöbə adı", "Cəm"]].copy()
                    agg.insert(0, "Ay", ay_ad)
                    sobe_monthly_rows.append(agg)

                cur = nxt

            # ---- Aylıq Özet (Dərman + Xidmət) ----
            summary_df = pd.DataFrame(summary_rows, columns=[
                "Ay",
                "Dərman Məbləğ (CƏM)","Dərman EDV-siz (CƏM)","Dərman İcbari",
                "Xidmət EDV-siz (Ödənişli)","Xidmət EDV-siz (Sığortalı)","Xidmət İcbari (CƏM)"
            ]).fillna(0)
            sheet_sum = "Aylıq Özet"
            summary_df.to_excel(writer, sheet_name=sheet_sum, index=False, startrow=2)
            ws = writer.sheets[sheet_sum]
            ws.write(0, 0, "Aylıq Yekun Özet — Dərman + Xidmət", title_fmt)
            ws.write(1, 0, f"Aralıq: {START_DATE.strftime('%d.%m.%Y')} – {(END_DATE - relativedelta(days=1)).strftime('%d.%m.%Y')}")
            for i, col in enumerate(summary_df.columns): ws.write(2, i, col, header_fmt)
            nrows = len(summary_df)
            for r in range(nrows):
                ws.set_row(3 + r, 18)
                for c, name in enumerate(summary_df.columns):
                    ws.write(3 + r, c, summary_df.iloc[r, c], money_fmt if name != "Ay" else text_fmt)
            total_row = 3 + nrows
            if nrows > 0:
                ws.write(total_row, 0, "CƏM", total_int_fmt)
                for cname in summary_df.columns[1:]:
                    c = summary_df.columns.get_loc(cname)
                    ws.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+nrows})", total_fmt)
            autofit_columns(writer, sheet_sum, summary_df)
            ws.autofilter(2, 0, max(2 + nrows, 3), len(summary_df.columns) - 1)
            ws.freeze_panes(3, 1)

            # ================== 3) İNDİ ÖN SİRA SHEETLƏRİN İÇİNİ DOLDUR ==================

            # ---- Şöbə Aylıq Cəm (Xidmət) — pivot + trend + sort desc (birinci sheet)
            if sobe_monthly_rows:
                long_df = pd.concat(sobe_monthly_rows, ignore_index=True)  # Ay, Şöbə adı, Cəm
                pivot = pd.pivot_table(long_df, index="Şöbə adı", columns="Ay", values="Cəm", aggfunc="sum", fill_value=0.0)
                cols = [m for m in months_order if m in pivot.columns]
                pivot = pivot.reindex(columns=cols)
                pivot["Yekun"] = pivot.sum(axis=1)
                pivot = pivot.sort_values("Yekun", ascending=False)

                inter_cols = []
                for i, m in enumerate(cols):
                    inter_cols.append(m)
                    if i > 0:
                        prev = cols[i-1]
                        dn = f"{m} Δ"
                        pivot[dn] = pivot[m] - pivot[prev]
                        inter_cols.append(dn)
                inter_cols.append("Yekun")
                pivot = pivot[inter_cols]

                pr = pivot.reset_index()
                pr.to_excel(writer, sheet_name=sheet_pivot_sobe, index=False, startrow=2)
                wp = writer.sheets[sheet_pivot_sobe]
                wp.write(0, 0, "Şöbə Aylıq Cəm — Xidmət (EDV-siz ödənişli + EDV-siz sığortalı + İcbari)", title_fmt)
                wp.write(1, 0, f"Aralıq: {START_DATE.strftime('%d.%m.%Y')} – {(END_DATE - relativedelta(days=1)).strftime('%d.%m.%Y')}")
                for i, col in enumerate(pr.columns): wp.write(2, i, col, header_fmt)

                npr = len(pr)
                trend_fmt = wb.add_format({"num_format": '[Green]"▲ " #,##0;[Red]"▼ " #,##0;[Blue]"– " #,##0',
                                           "align":"center","border":1})
                autofit_columns(writer, sheet_pivot_sobe, pr)
                trend_cols = [pr.columns.get_loc(c) for c in pr.columns if c.endswith(" Δ")]

                for r in range(npr):
                    wp.set_row(3 + r, 18)
                    for c, name in enumerate(pr.columns):
                        v = pr.iloc[r, c]
                        if name == "Şöbə adı": wp.write(3 + r, c, v, text_fmt)
                        elif name.endswith(" Δ"): wp.write_number(3 + r, c, float(v), trend_fmt)
                        else: wp.write_number(3 + r, c, float(v), money_fmt)

                total_row = 3 + npr
                if npr > 0:
                    wp.write(total_row, 0, "CƏM", total_int_fmt)
                    for c, name in enumerate(pr.columns[1:], start=1):
                        if name.endswith(" Δ"): wp.write(total_row, c, "", total_int_fmt)
                        else: wp.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+npr})", total_fmt)

                for c in trend_cols: wp.set_column(c, c, 5)
                wp.autofilter(2, 0, max(2 + npr, 3), len(pr.columns) - 1)
                wp.freeze_panes(3, 1)
                for c, name in enumerate(pr.columns[1:], start=1):
                    if not name.endswith(" Δ"):
                        wp.conditional_format(3, c, 2 + npr, c, {"type": "data_bar"})

            # ---- Depo Aylıq Cəm (Dərman) — pivot + trend + sort desc (ikinci sheet)
            if depo_monthly_rows:
                long_df = pd.concat(depo_monthly_rows, ignore_index=True)  # Ay, Depo, Cəm
                pivot = pd.pivot_table(long_df, index="Depo", columns="Ay", values="Cəm", aggfunc="sum", fill_value=0.0)
                cols = [m for m in months_order if m in pivot.columns]
                pivot = pivot.reindex(columns=cols)
                pivot["Yekun"] = pivot.sum(axis=1)
                pivot = pivot.sort_values("Yekun", ascending=False)

                inter_cols = []
                for i, m in enumerate(cols):
                    inter_cols.append(m)
                    if i > 0:
                        prev = cols[i-1]
                        dn = f"{m} Δ"
                        pivot[dn] = pivot[m] - pivot[prev]
                        inter_cols.append(dn)
                inter_cols.append("Yekun")
                pivot = pivot[inter_cols]

                pr = pivot.reset_index()
                pr.to_excel(writer, sheet_name=sheet_pivot_depo, index=False, startrow=2)
                wp = writer.sheets[sheet_pivot_depo]
                wp.write(0, 0, "Depo Aylıq Cəm — Dərman (EDV-siz + İcbari)", title_fmt)
                wp.write(1, 0, f"Aralıq: {START_DATE.strftime('%d.%m.%Y')} – {(END_DATE - relativedelta(days=1)).strftime('%d.%m.%Y')}")
                for i, col in enumerate(pr.columns): wp.write(2, i, col, header_fmt)

                npr = len(pr)
                trend_fmt = wb.add_format({"num_format": '[Green]"▲ " #,##0;[Red]"▼ " #,##0;[Blue]"– " #,##0',
                                           "align":"center","border":1})
                autofit_columns(writer, sheet_pivot_depo, pr)
                trend_cols = [pr.columns.get_loc(c) for c in pr.columns if c.endswith(" Δ")]

                for r in range(npr):
                    wp.set_row(3 + r, 18)
                    for c, name in enumerate(pr.columns):
                        v = pr.iloc[r, c]
                        if name == "Depo": wp.write(3 + r, c, v, text_fmt)
                        elif name.endswith(" Δ"): wp.write_number(3 + r, c, float(v), trend_fmt)
                        else: wp.write_number(3 + r, c, float(v), money_fmt)

                total_row = 3 + npr
                if npr > 0:
                    wp.write(total_row, 0, "CƏM", total_int_fmt)
                    for c, name in enumerate(pr.columns[1:], start=1):
                        if name.endswith(" Δ"): wp.write(total_row, c, "", total_int_fmt)
                        else: wp.write_formula(total_row, c, f"=SUM({xl_col(c)}4:{xl_col(c)}{3+npr})", total_fmt)

                for c in trend_cols: wp.set_column(c, c, 5)
                wp.autofilter(2, 0, max(2 + npr, 3), len(pr.columns) - 1)
                wp.freeze_panes(3, 1)
                for c, name in enumerate(pr.columns[1:], start=1):
                    if not name.endswith(" Δ"):
                        wp.conditional_format(3, c, 2 + npr, c, {"type": "data_bar"})

            # ---- Qeyd (üçüncü sheet) — rəhbərlik üçün qısa mətn (tam görünən versiya)
            wsq = writer.sheets[sheet_note]
            title_fmt2    = wb.add_format({"bold": True, "font_size": 14})
            subtitle_fmt2 = wb.add_format({"italic": True, "font_size": 10, "font_color": "#666666"})
            wrap_fmt      = wb.add_format({"text_wrap": True, "valign": "top", "border": 1})
            box_warn      = wb.add_format({"text_wrap": True, "valign": "top", "border": 1, "bg_color": "#F8CBAD"})  # açıq qırmızı
            box_info      = wb.add_format({"text_wrap": True, "valign": "top", "border": 1, "bg_color": "#FFF2CC"})  # sarı
            box_ok        = wb.add_format({"text_wrap": True, "valign": "top", "border": 1, "bg_color": "#E2EFDA"})  # yaşıl

            wsq.set_column(0, 0, 110)
            wsq.set_row(0, 30)
            wsq.write(0, 0, "Hesabatın izahı ", title_fmt2)
            wsq.write(1, 0, f"Aralıq: {START_DATE.strftime('%d.%m.%Y')} – {(END_DATE - relativedelta(days=1)).strftime('%d.%m.%Y')}", subtitle_fmt2)

            def write_box(row_start: int, row_span: int, text: str, cell_fmt, height: int = 28):
                """
                Bir neçə sətri birləşdirib (merge) mətn yazır və hər sətrin hündürlüyünü artırır.
                """
                row_end = row_start + row_span - 1
                wsq.merge_range(row_start, 0, row_end, 0, text, cell_fmt)
                for r in range(row_start, row_end + 1):
                    wsq.set_row(r, height)

            # 🔹 Məqsəd
            write_box(
                row_start=3, row_span=4,
                text="🔎 Məqsəd: Şöbə və Depo üzrə aylıq maliyyə nəticələrini sadə, rəngli və müqayisə edilə bilən formatda göstərmək.",
                cell_fmt=box_ok, height=30
            )

            # 🔸 Hesabat göstəriciləri
            write_box(
                row_start=8, row_span=7,
                text=(
                    "📊 Hesabat göstəriciləri:\n\n"
                    "• Xidmət məbləği\n"
                    "• Təşkilat məbləği\n"
                    "• Dərman məbləği\n"
                    "• İcbari məbləği"
                ),
                cell_fmt=box_info, height=28
            )

            # ⚠️ ƏDV tətbiqi
            write_box(
                row_start=16, row_span=7,
                text=(
                    "💡 ƏDV tətbiqi:\n\n"
                    "• İcbari məbləğdən ƏDV çıxılmır.\n"
                    "• Təşkilat və Ödənişli məbləğlərdən hər biri üçün ƏDV çıxılaraq hesablanır."
                ),
                cell_fmt=box_warn, height=28
            )

            # 🧾 Təşkilat məbləği qaydası
            write_box(
                row_start=24, row_span=6,
                text=(
                    "🧾 Təşkilat məbləği qaydası:\n\n"
                    "• Təşkilat məbləği hesablanarkən İcbari məbləğ nəzərə alınmır, ayrıca hesablanır."
                ),
                cell_fmt=box_info, height=28
            )

            # ⚙️ Xidmət statusu
            write_box(
                row_start=31, row_span=6,
                text=(
                    "⚙️ Xidmət statusu:\n\n"
                    "• Hesabatda xidmətlərin ödəniş statusu (ödənmiş/ödənməmiş) nəzərə alınmır."
                ),
                cell_fmt=wrap_fmt, height=28
            )

    print(f"✅ Fayl yaradıldı: {out_xlsx}")

if __name__ == "__main__":
    main()
