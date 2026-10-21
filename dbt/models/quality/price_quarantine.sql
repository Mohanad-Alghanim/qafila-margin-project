WITH supplier_prices AS (

    SELECT *
    FROM {{ ref('stg_supplier_prices') }}

),

classified AS (

    SELECT
        s.supplier_id,
        s.sku,
        s.product_name_ar,
        s.valid_from,
        s.min_order_qty,
        s.currency,
        s.source_file,

        s.raw_unit_cost,
        CAST(s.raw_unit_cost AS VARCHAR) AS raw_cost,

        s.normalized_cost,
        s.unit_cost,

        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM {{ source('raw', 'products') }} AS p
                WHERE TRIM(p.sku) = s.sku
            )
                THEN 'unknown_sku'

            WHEN s.unit_cost IS NULL
                 OR s.unit_cost <= 0
                THEN 'invalid_cost'

            ELSE NULL
        END AS reason

    FROM supplier_prices AS s

)

SELECT *
FROM classified
WHERE reason IS NOT NULL
