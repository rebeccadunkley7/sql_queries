/*
COMMERCIAL HALO EFFECT ANALYSIS

Scenario: A dairy manufacturer wants to understand the commercial impact of past to pop-up & consumer events to evaluate whether to continue with the current IRL strategy.
Solution: This uses the event start dates & cities to calculate a 3M moving average of customer acquisition, sales & website visitors in the months before & after events took place.

*/

WITH orders as 
(
select       o.customer_id ,o.order_sequence ,o.order_date ,item_quantity ,order_net_rev ,shopify_order_name
            ,case     when lower(o.shipping_city) like '%bourton%' then ‘BOURTON’
                      when lower(o.shipping_city) like '%burford%' then 'BURFORD'
                      when lower(o.shipping_city) like '%tetbury%' then 'TETBURY'
                      when lower(o.shipping_city) like '%cirencester%' then 'CIRENCESTER'
                      when lower(o.shipping_city) like '%chipping camden%' then 'CHIPPING CAMDEN'
                      else null end shipping_city
            ,e.event_name
            ,e.event_start_date popup_date
            ,datediff('months',popup_date,order_date) months_to_popup_event
            
from         sales.customer_orders o

left join    (select event_name, event_start_date, town from marketing.events_lookup where event_name = 'Bourton_Market') e 
                
where        ((lower(shipping_city) like '%bourton%' and lower(order_shipping_county) =’gloucestershire’) or
             (lower(shipping_city) like '%chipping camden%' and lower(order_shipping_county) = ’gloucestershire’) or
             (lower(shipping_city) like '%burford%' and lower(order_shipping_county) = ’gloucestershire’) or 
             (lower(shipping_city) like '%tetbury%' and lower(order_shipping_county) = ’gloucestershire’) or 
             (lower(shipping_city) like '%cirencester%' and lower(order_shipping_county) = ’gloucestershire’) )

             and (item_quantity > 0)
             and months_to_popup_event between -12 and 12
             
order by 6 

)

,halo_stats_by_month_commercial as 
(
select       event_name ,shipping_city ,months_to_popup_event 
            ,count(distinct customer_id) distinct_customers ,count(distinct case when order_sequence = 1 then customer_id else null end) new_customers
            ,sum(order_net_rev) total_sales ,sum(case when order_sequence = 1 then order_net_rev else null end) new_customer_sales
            ,count(shopify_order_name) total_orders ,count(case when order_sequence = 1 then shopify_order_name else null end) new_customer_orders
            
from         orders
where        months_to_popup_event between -12 and 12
group by     1,2,3
order by     1,3
)

,session_data as 
(
select     top 10000
            case      when lower(geonetwork_city) like '%bourton%' then 'BOURTON'
                      when lower(geonetwork_city) like '%chipping camden%' then 'CHIPPING CAMDEN'
                      when lower(geonetwork_city) like '%burford%' then 'BURFORD'
                      when lower(geonetwork_city) like '%tetbury%' then 'TETBURY'
                      when lower(geonetwork_city) like '%cirencester%' then 'CIRENCESTER'
                     else null end browsing_city
            ,e.event_name
            ,e.event_start_date popup_date
            ,datediff('months',popup_date,g.visit_date) months_to_popup_event
            ,g.*
        
from         website.sessions_master g
left join    (select event_name, event_start_date, city from marketing.events_lookup where event_name ='Bourton_Market ') e 
                                     
where       ( lower(geonetwork_city) like '%bourton%' or lower(geonetwork_city) like '%chipping camden%'
            or lower(geonetwork_city) like '%burford%' or lower(geonetwork_city) like '%tetbury%'  
            or lower(geonetwork_city) like '%cirencester%' )
            
            and (months_to_popup_event between -12 and 12)
            
order by     customer_id ,visit_start_time

)

,halo_stats_by_month_session as 
(
select       event_name ,browsing_city ,months_to_popup_event 
            ,count(visit_key) total_sessions ,count(distinct case when customer_id is null then visitor_id else customer_id end) total_visitors_estimate ,sum(transactions) total_orders_ga ,round(total_orders_ga/total_sessions,3) ga_cvr_approx
            
from         session_data
where        months_to_popup_event between -12 and 12
group by     1,2,3
order by     2,3
)

,session_commercial_halo_merge as
(
select        c.* ,s.total_sessions ,s.total_visitors_estimate ,s.total_orders_ga ,s.ga_cvr_approx
from          halo_stats_by_month_commercial c
left join     halo_stats_by_month_session s on c.shipping_city = s.browsing_city and c.months_to_popup_event = s.months_to_popup_event
)

select 	h.* 
,round(avg(total_visitors_estimate) over (partition by event_name order by months_to_popup_event rows between 2 preceding and current row)) avg_visitors_l3m
,round(avg(new_customers) over (partition by event_name order by months_to_popup_event rows between 2 preceding and current row)) avg_new_customers_l3m
,round(avg(total_sales) over (partition by event_name order by months_to_popup_event rows between 2 preceding and current row),2) avg_net_sales_l3m

from session_commercial_halo_merge h
order by shipping_city ,months_to_popup_event

;

;

