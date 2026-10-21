
SELECT
    sku,
    valid_from,
    valid_to
FROM {{ ref('product_cost_history') }}
WHERE valid_to IS NOT NULL
  AND valid_to <= valid_from
