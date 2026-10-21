
WITH source_data AS (

    SELECT
        supplier_id,
        TRIM(sku) AS sku,
        product_name_ar,
        valid_from,
        min_order_qty,
        currency,
        source_file,

        COALESCE(unit_cost, cost_sar) AS raw_unit_cost

    FROM {{ source('raw', 'supplier_prices') }}

),

normalized AS (

    SELECT
        *,

        TRANSLATE(
            TRIM(raw_unit_cost),
            '٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫٬',
            '01234567890123456789.,'
        ) AS translated_cost

    FROM source_data

),

cleaned AS (

    SELECT
        *,

        REPLACE(
            translated_cost,
            ',',
            '.'
        ) AS normalized_cost

    FROM normalized

)

SELECT
    supplier_id,
    sku,
    product_name_ar,
    TRY_CAST(valid_from AS DATE) AS valid_from,
    TRY_CAST(min_order_qty AS INTEGER) AS min_order_qty,
    currency,
    source_file,
    raw_unit_cost,
    normalized_cost,
    TRY_CAST(normalized_cost AS DECIMAL(18, 2)) AS unit_cost

FROM cleaned
