
SELECT
    sku,
    unit_cost
FROM {{ ref('product_cost_history') }}
WHERE unit_cost <= 0
