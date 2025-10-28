SELECT 
    SR_ADI,
    "2025-01" AS YANVAR,
    "2025-02" AS FEVRAL,
    "2025-03" AS MART,
    "2025-04" AS APREL,
    "2025-05" AS MAY,
    "2025-06" AS IYUN,  -- 'İ' əvəzinə 'I'
    "2025-07" AS IYUL,  -- 'İ' əvəzinə 'I'
    "2025-08" AS AVQUST,
    "2025-09" AS SENTYABR,
    "2025-10" AS OKTYABR,
    -- 'Ə' əvəzinə 'E' və ya 'C' əvəzinə 'CEM' və ya 'TOTAL'
    ("2025-01" + "2025-02" + "2025-03" + "2025-04" + "2025-05" + "2025-06" + "2025-07" + "2025-08" + "2025-09" + "2025-10") AS CEMI
FROM (
    SELECT 
        t.SR_ADI,
        TO_CHAR(t.HI_TARIH, 'YYYY-MM') AS AY,
        COUNT(DISTINCT t.HK_ID) AS CNT
    FROM fonethbys.V_IST_GENEL_HIZMET t
    WHERE t.HI_TARIH >= DATE '2025-01-01'
    GROUP BY t.SR_ADI, TO_CHAR(t.HI_TARIH, 'YYYY-MM')
)
PIVOT (
    SUM(CNT)
    FOR AY IN (
        '2025-01' AS "2025-01",
        '2025-02' AS "2025-02",
        '2025-03' AS "2025-03",
        '2025-04' AS "2025-04",
        '2025-05' AS "2025-05",
        '2025-06' AS "2025-06",
        '2025-07' AS "2025-07",
        '2025-08' AS "2025-08",
        '2025-09' AS "2025-09",
        '2025-10' AS "2025-10"
    )
)
ORDER BY SR_ADI;
