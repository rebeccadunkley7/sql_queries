/*
CUSTOMER VALUE - COHORT ANALYSIS

This query would be used in a customer level feature store to append onto different types of cohort analysis with many different use cases.

Examples of Use Cases:
* Segmenting customers by short, medium & long term value.
* Conducting cluster analysis on value segments to identify high value bs low value cohorts of customers.
* Use high value customers in lookalike targetting for marketing channels.

*/


with item_level_data as 
-- calculating the costs related to items sold and returned
(
SELECT    wix_order_name, i.customer_id ,sum(cost_of_goods_gbp) cogs
          ,ifnull(sum((nullif(item_net_rev,0) / nullif(item_quantity,0)) * items_returned),0) returns
          ,ifnull(sum((nullif(cost_of_goods_gbp,0) / nullif(item_quantity,0)) * items_returned),0) cost_of_goods_returns
FROM      sales.items_sold 
group by  wix_order_name, customer_id
)     

-- aggregating total commercial metrics at a customer level, aggregated by days since first purchase as interim CLTV calc
SELECT       o.customer_id ,c.infilled_gender ,o.acquisition_date 
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 731 THEN order_gross_sales ELSE NULL END) total_revenue_24m
            ,COUNT(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 731 THEN o.wix_order_name ELSE NULL END) total_orders_24m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 731 THEN item_quantity ELSE NULL END) total_items_24m
            ,ROUND(total_revenue_24m/total_orders_24m,4) AOV_24m
            ,ROUND(total_items_24m/total_orders_24m,1) items_per_order_24m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 731 THEN order_net_sales ELSE NULL END) total_order_net_revenue_24m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 731 THEN (order_net_sales - im.cogs - im.returns + im.cost_of_goods_returns) ELSE NULL END) GP1_24m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 366 THEN order_gross_sales ELSE NULL END) total_revenue_12m
            ,COUNT(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 366 THEN o.wix_order_name ELSE NULL END) total_orders_12m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 366 THEN item_quantity ELSE NULL END) total_items_12m
            ,ROUND(total_revenue_12m/total_orders_12m,4) AOV_12m
            ,ROUND(total_items_12m/total_orders_12m,1) items_per_order_12m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 366 THEN order_net_sales ELSE NULL END) total_order_net_revenue_12m
            ,SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) < 366 THEN (order_net_sales - im.cogs - im.returns + im.cost_of_goods_returns) ELSE NULL END) GP1_12m
            ,CASE WHEN (SUM(CASE WHEN datediff(DAY, o.acquisition_date, o.order_date) > 365 THEN 1 ELSE NULL END)) IS NOT NULL THEN 1 ELSE NULL END retained_customer
            ,CASE WHEN datediff(DAY, o.acquisition_date, '2022-12-31') > 365 THEN 1 ELSE NULL END retained_eligible_customer

FROM        sales.customer_orders o
LEFT JOIN   sales.customer_database c ON c.customer_id = o.customer_id
LEFT JOIN   item_level_data im ON o.customer_id = im.customer_id
GROUP BY    1,2,o.acquisition_date

)

;
