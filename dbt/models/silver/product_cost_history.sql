WITH valid_prices AS (

    SELECT
        s.sku,
        s.supplier_id,
        s.valid_from,
        s.unit_cost,
        s.source_file

    FROM {{ ref('stg_supplier_prices') }} AS s

    WHERE s.unit_cost > 0
      AND s.valid_from IS NOT NULL

      AND EXISTS (
          SELECT 1
          FROM {{ source('raw', 'products') }} AS p
          WHERE TRIM(p.sku) = s.sku
      )

),

ranked_prices AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY sku, valid_from
            ORDER BY source_file DESC
        ) AS row_num

    FROM valid_prices

),

deduplicated AS (

    SELECT
        sku,
        supplier_id,
        valid_from,
        unit_cost,
        source_file

    FROM ranked_prices

    WHERE row_num = 1

),

previous_costs AS (

    SELECT
        *,

        LAG(unit_cost) OVER (
            PARTITION BY sku
            ORDER BY valid_from
        ) AS previous_unit_cost

    FROM deduplicated

),

changes_only AS (

    SELECT
        sku,
        supplier_id,
        valid_from,
        unit_cost,
        source_file

    FROM previous_costs

    WHERE previous_unit_cost IS NULL
       OR unit_cost <> previous_unit_cost

),

cost_history AS (

    SELECT
        sku,
        supplier_id,
        unit_cost,
        valid_from,

        LEAD(valid_from) OVER (
            PARTITION BY sku
            ORDER BY valid_from
        ) AS valid_to,

        source_file

    FROM changes_only

)

SELECT
    sku,
    supplier_id,

    unit_cost,
    CAST(unit_cost AS DOUBLE) AS unit_cost_sar,

    valid_from,
    valid_to,

    CAST(valid_to IS NULL AS BOOLEAN) AS is_current,

    source_file

FROM cost_history
