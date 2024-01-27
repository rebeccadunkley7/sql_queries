/*
IDENTIFYING CUSTOMER INTERACTIONS BETWEEN ORDERS

Scenario: A marketing department would like to understand what drives customers purchases.
Solution: This logic creates on an order level features table which seeks to map interactions across channels & website behaviours.

Example Uses For Logic:
* Conduct high level analysis on trends between customers with shorter purchase journey's, looking at similarities & differences between interactions between orders.
* Draw patterns between which types of features lead to conversion vs consideration.
* Understand the typical time between orders for different customer cohorts, and typical number of website sessions during consideration phase.
*/

create or replace view marketing.interactions_between_orders as (

with customer_order_history as
-- creating additional fields from the order history table to enable analysis on interactions between a customers orders
(
select     s.customer_id ,s.order_sequence ,s.order_date ,s.order_date_nam ,s.wix_order_stamp ,s.wix_order_name ,s.wix_order_id ,s.order_flag_id ,s.fully_returned 
          ,s.item_quantity ,s.order_gross_total_gbp ,s.order_net_sales_gbp ,s.shipping_country ,s.order_store 

          -- joined values
          ,q.acquisition_date ,c.region ,c.global_group
  
          -- logic for custom fields
          ,case when s.order_date > order_date_nam then order_date_nam else order_date end order_date_local
          ,case when s.order_sequence > 1 then LAG(s.order_date,1) OVER (PARTITION by s.customer_id order by s.customer_id, s.order_sequence asc) else null end prior_order_date
          ,case when s.order_sequence > 1 then LAG(s.wix_order_stamp,1) OVER (PARTITION by s.customer_id order by s.customer_id, s.order_sequence asc) else null end prior_order_date_timestamp
          ,case when s.order_sequence > 1 then fy.fiscal_year else null end repeat_order_fy
          ,datediff(DAY
                   ,LAG(s.order_date_local,1) OVER (PARTITION by s.customer_id order by s.customer_id ,s.order_sequence asc)
                   ,s.order_date_local) days_btwn_orders

from      sales.customer_orders s
  
left join   lookup.regional_index c ON upper(s.shipping_country) = c.shipping_country -- company region groupings
left join   lookup.fiscal_dates fy ON s.order_date = fy.cal_date -- fiscal dates 
left join   (select customer_id ,min(order_date) acquisition_date from sales.customer_orders) q ON q.customer_id = s.customer_id -- acquisition dates

where       acquisition_date between '2022-01-01' AND current_date()

)


,base_table as -- creating list of customer orders that meet the criteria of the analysis
(
        with customer_order_dupes as -- excluding customers where order id's are not unique
            (
            select customer_id, wix_order_name, count(distinct order_sequence)
            from customer_order_history
            where order_sequence in (1,2)         
            group by 1,2
            having count(distinct order_sequence) > 1
            )

        select     customer_id 
                   ,order_sequence
                   ,row_number() over (partition by customer_id order by wix_order_stamp, order_sequence) order_sequence_new
                   ,order_sequence - order_sequence_new as diff_seq
                   ,order_date ,order_date_us ,order_date_local
                   ,wix_order_stamp ,acquisition_date ,prior_order_date, prior_order_date_timestamp, repeat_fy ,days_since_prior_order ,wix_order_name ,order_flag_id
                   ,fully_returned ,item_quantity ,order_gross_total_gbp ,order_net_sales_gbp ,shipping_country
                   ,order_store ,region ,global_group ,day_type
        from       customer_order_history
        where      wix_order_name not in (select wix_order_name from customer_order_dupes) and wix_order_stamp is not null 
        order by   customer_id, order_date_local, order_sequence
    )


--- PRODUCT SATISFACTION / CUSTOMER EXPERIENCE
  
,returns_data as
-- how many items from teh order have had returns processed?
(
select     wix_order_id ,SUM(quantity_returned) items_returned, max(LEFT(refund_date,10)) refund_date --,transaction_id
from       master_prod_db.gymshark.refunds_master
where      wix_order_id in (select wix_order_name from base_table)
group by   1
)
          
,customer_review as
(
select     customer_id, submitted_at, rating
from       website.customer_ratings
group by   1,2,3
)

--- MARKETING / ACCOUNT OPT IN STATUSES

,opt_in_flag as 
(
select         cm.customer_id, z.previous_opt_in, z.new_opt_in, z.updated_on as change_date, count(*) as count
from           crm.emain_opt_in_logging z
left join      sales.customer_details cm using (email_address)
where          new_opt_in is not null
group by       1,2,3,4
having         count = 1
)

,account_created as
( 
select         c.customer_id, a.updated_at, case when a.new_account_state = ‘activated’ then 'Y' else 'N' end as account_status 
from           website.customer_account_status a
left join      sales.customer_details c using (email)
where          customer_id in (select customer_id from base_table)
group by       1,2,3
)
  
      
--- APP INTERACTIONS
          
,ecomm_app_download as
(
select         customer_id, min(session_date) download_date  
from           ecomm_app.visit_master
group by       1
)

          
--- BROWSING BEHAVIOUR 
  
,website_visit_data as
-- website visit data to be used in following CTE's to create browsing features 
(
        with wix_dupes as -- exclusion to remove customers that have orders attributed to multiple sessions
        (
        select     customer_id, wix_order_name ,count(wix_order_name)
        from       website.sessions_master
        where      wix_order_name is not null
        group by   1,2
        having     count(wix_order_name) > 1
        )

select    *
from      website.sessions_master
where     customer_id not in (select customer_id from wix_dupes)
)

,conversion_session_data as
-- what did the converting session look like for that customer (e.g. traffic source & medium, interactions of intent etc)
(
select     * 
from       website_visit_data
where      wix_order_name in (select wix_order_name from base_table)
)

,website_distinct_data as
-- first website visit dates
(
select     customer_id, visit_date first_visit_date, visit_start_time first_visit_timestamp 
from       website_visit_data 
where      web_visit_number = 1 
group by   1,2,3
)

          
-- PURCHASE INTENT 
          
,wishlist_view as
(
select        s.customer_id, s.wix_order_name, s.visit_start_time, w.dates, w.visitor_id, w.visitid, count(distinct w.productsku) as num_prod_added
from          website.wishlist_logging w
left join     website.sessions_master s on s.visitor_id = w.visitor_id and s.visit_id = w.visitid and s.visit_date = w.dates
group by      1,2,3,4,5,6
)

,added_to_cart as
(
select         s.customer_id, s.visit_date, s.wix_order_name, pv.visit_key, pv.visit_start_time, count(*) as total_cart_adds
from           website.product_interactions pv
left join      website.sessions_master s using (visit_key, visit_start_time) 
where          event_action = ‘Added_To_Cart’ and customer_id in (select customer_id from base_table)
group by       1,2,3,4,5
)

          
-- CONTENT CONSUMPTION
          
,blog_visit_flag as --cte to control filters out of sessions table
(
select          customer_id, wix_order_name, visit_date, visit_start_time, count(distinct customer_id) as blog_visit_flag
from            website.sessions_master
where           array_to_string(all_subdomains, ' , ') like '%central%' and customer_id in (select customer_id from base_table)
group by        1,2,3,4
)

  

,feature_joins as
(
select         b.customer_id
              ,b.order_sequence_new
              ,b.wix_order_stamp 
              ,b.wix_order_name
              ,w2.first_visit_date as first_visit_date
              ,w2.first_visit_timestamp
              ,case when order_sequence_new = 1 and first_visit_timestamp <= wix_order_stamp then first_visit_timestamp else prior_order_date_timestamp end prior_order_or_first_visit_timestamp
              ,r2.items_returned
              ,case when items_returned = item_quantity then 'FULL' when items_returned < item_quantity then 'PART' else NULL end as full_or_part_return
              ,cm.customer_gender
              ,w.channel_grouping conversion_channel
          
              -- order level feature joins
              ,case when bvf.visit_start_time between prior_order_or_first_visit_timestamp and wix_order_stamp then 'Y'else 'N' end as blog_visit_flag
              ,sum(case when r.submitted_at is not null then 1 else 0 end) as total_reviews
              ,round(avg(r.rating),1) as Avg_Rating
              ,case when (ea.customer_id is not null) then 'Y' else 'N' end ecom_app_download 
              ,case when (ta.customer_id is not null) then 'Y' else 'N' end train_app_download 
              ,case when wv.visit_start_time between prior_order_or_first_visit_timestamp and wix_order_stamp then 'Y' else 'N' end as wishlist_add 
              ,count(num_prod_added) as count_wishlist_add
              ,sum(coalesce(num_prod_added, 0)) as wishlist_num_prod_added
              ,coalesce(sum(total_cart_adds),0) as total_add_to_carts 
              ,coalesce(sum(bs.bounces), 0) total_bounced_sessions 
              ,count(bs.*) total_sessions
              ,total_bounced_sessions/total_sessions*100 bounce_rate
              ,max(opt.change_date) as max_date
              ,max(opt.new_opt_in) as latest_opt_status
              ,max(ac.updated_at) as updated_at
              ,max(coalesce(ac.account_status,'N')) as account_status
              ,cg.mail_control 
    
from           base_table b

left join   (select customer_id, web_visit_number ,visit_date from website_visit_data g where web_visit_number = 1) g1 on g1.customer_id = b.customer_id -- first website visit
left join   returns_data r2 on b.wix_order_name = r2.wix_order_id --refunds
left join   conversion_session_data w on b.customer_id = w.customer_id and w.wix_order_name = b.wix_order_name -- converting session
left join   website_distinct_data w2 on b.customer_id = w2.customer_id
left join   sales.customer_details cm on b.customer_id = cm.customer_id 

-- sequential joins on customer, order & timestamps               
left join   blog_visit_flag bvf on bvf.customer_id = b.customer_id  and bvf.visit_start_time between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   left_review r on r.customer_id = b.customer_id and r.submitted_at between prior_order_or_first_visit_timestamp and wix_order_stamp 
left join   ecom_download_view ea on ea.customer_id = b.customer_id and ea.download_date between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   train_download_view ta on ta.customer_id = b.customer_id and ta.download_date between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   wishlist_view wv on wv.customer_id = b.customer_id and wv.visit_start_time between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   added_to_cart atc on atc.customer_id = b.customer_id and atc.visit_start_time between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   bounces_and_total_sessions_on_purchase bs on bs.customer_id = b.customer_id and bs.visit_start_time between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   opt_in_flag opt on opt.customer_id = b.customer_id and opt.change_date between prior_order_or_first_visit_timestamp and wix_order_stamp
left join   account_created ac on ac.customer_id = b.customer_id and ac.updated_at between prior_order_or_first_visit_timestamp and wix_order_stamp

group by 1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,28
)

select * from feature_joins 
          
)

;
