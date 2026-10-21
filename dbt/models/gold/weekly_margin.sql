
WITH eligible_orders AS (

    SELECT
        o.order_id,
        s.city,

        CAST(
            timezone(
                'Asia/Riyadh',
                timezone(
                    'UTC',
                    TRY_CAST(o.created_at AS TIMESTAMP)
                )
            ) AS DATE
        ) AS order_date

    FROM {{ source('raw', 'orders') }} AS o

    INNER JOIN {{ source('raw', 'stores') }} AS s
        ON o.store_id = s.store_id

    WHERE o.status = 'delivered'

),

dated_orders AS (

    SELECT
        order_id,
        city,
        order_date,

        CAST(
            EXTRACT(WEEK FROM order_date)
            AS INTEGER
        ) AS iso_week

    FROM eligible_orders

    WHERE order_date >= DATE '2026-08-31'

),

sales_lines AS (

    SELECT
        o.order_id,
        o.city,
        o.order_date,
        o.iso_week,
        i.line_no,
        p.sku,
        p.category,
        i.quantity,

        CAST(
            ROUND(
                i.quantity * i.unit_price
                - COALESCE(i.discount, 0),
                2
            ) AS DECIMAL(18, 2)
        ) AS line_revenue

    FROM dated_orders AS o

    INNER JOIN {{ source('raw', 'order_items') }} AS i
        ON o.order_id = i.order_id

    INNER JOIN {{ source('raw', 'products') }} AS p
        ON i.product_id = p.product_id

    WHERE i.quantity > 0

),

costed_lines AS (

    SELECT
        s.*,
        h.unit_cost,

        CASE
            WHEN h.unit_cost IS NOT NULL
            THEN CAST(
                ROUND(s.quantity * h.unit_cost, 2)
                AS DECIMAL(18, 2)
            )
            ELSE NULL
        END AS line_cost

    FROM sales_lines AS s

    LEFT JOIN {{ ref('product_cost_history') }} AS h
        ON s.sku = h.sku
       AND s.order_date >= h.valid_from
       AND (
           s.order_date < h.valid_to
           OR h.valid_to IS NULL
       )

),

weekly_aggregates AS (

    SELECT
        iso_week,
        city,
        category,

        COUNT(*) AS lines,

        CAST(
            SUM(line_revenue)
            AS DECIMAL(18, 2)
        ) AS revenue_sar,

        CAST(
            SUM(
                CASE
                    WHEN unit_cost IS NOT NULL
                    THEN line_revenue
                    ELSE 0
                END
            ) AS DECIMAL(18, 2)
        ) AS costed_revenue_sar,

        CAST(
            COALESCE(SUM(line_cost), 0)
            AS DECIMAL(18, 2)
        ) AS cost_sar,

        COUNT(*) FILTER (
            WHERE unit_cost IS NULL
        ) AS uncosted_lines

    FROM costed_lines

    GROUP BY
        iso_week,
        city,
        category

)

SELECT
    iso_week,
    city,
    category,
    lines,
    revenue_sar,
    costed_revenue_sar,
    cost_sar,

    CAST(
        costed_revenue_sar - cost_sar
        AS DECIMAL(18, 2)
    ) AS margin_sar,

    uncosted_lines

FROM weekly_aggregates
