-- Dimensão Calendário - De 2010 a 2035

WITH RECURSIVE calendario AS (
    SELECT DATE '2010-01-01'::TIMESTAMP AS data
    UNION ALL
    SELECT data + INTERVAL '1 day'
    FROM calendario
    WHERE data < '2035-12-31'::TIMESTAMP
)

SELECT
    TO_CHAR(data, 'YYYYMMDD')::INTEGER AS sk_data,
    data AS data_completa,
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(DAY FROM data) AS dia,
    TO_CHAR(data, 'TMMonth') AS nome_mes,
    TO_CHAR(data, 'TMDay') AS nome_dia_semana,
    EXTRACT(DOW FROM data) AS dia_semana,
    EXTRACT(QUARTER FROM data) AS trimestre,
    EXTRACT(WEEK FROM data) AS semana_ano,
    CASE
        WHEN EXTRACT(MONTH FROM data) IN (1, 2, 3) THEN 1
        WHEN EXTRACT(MONTH FROM data) IN (4, 5, 6) THEN 2
        WHEN EXTRACT(MONTH FROM data) IN (7, 8, 9) THEN 3
        ELSE 4
    END AS quadrimestre,
    CASE
        WHEN EXTRACT(MONTH FROM data) IN (1, 2, 3, 4, 5, 6) THEN 1
        ELSE 2
    END AS semestre,
    CASE
        WHEN EXTRACT(ISODOW FROM data) IN (6, 7) THEN TRUE
        ELSE FALSE
    END AS e_fim_de_semana,
    CASE
        WHEN EXTRACT(MONTH FROM data) = 12 AND EXTRACT(DAY FROM data) = 25 THEN TRUE
        ELSE FALSE
    END AS e_natal,
    CASE
        WHEN EXTRACT(MONTH FROM data) = 1 AND EXTRACT(DAY FROM data) = 1 THEN TRUE
        ELSE FALSE
    END AS e_ano_novo
FROM calendario
ORDER BY data
