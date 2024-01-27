/*

CLEANSING PURCHASE ORDERS USING STRING MANIPULATION & JOINS
------------------------------------------------------------
Scenario: An Influencer Marketing department is struggling to understand the ROAS of their activations for key periods.
Solution: To unlock ability to track cost metrics, the below query seeks to attribute payments to the correct influencer / agency & month.

*/

create table influencer_marketing.talent_payments as (

with finance_data as 
-- exporting only relevant purchase orders by category from the source table
(
select        *
from          FINANCE.INFLUENCER_PURCHASE_ORDERS
where         purchase_order_description in ('Influencer Team : Monthly Salary Payments' 
                                              ,'Influencer Team : Ambassador Monthly Salary Payments'
                                              ,'Influencer Team : Influencer Team : Influencer Campaigns'
                                              ,'Influencer Team : Influencer Team : Fixed One Year Fees'
                                              ,'Influencer Team : Influencer Team : Fixed Three Month Fees'
                                              ,'Influencer Team : Influencer Team : Commissions'
                                              ,'Influencer Team : Influencer Team : Bonus')
)

,month_year_concat as 
-- using string descriptions to extract the actual month & year the marketing spend is related to (rather than date created).
--  (e.g. if a purchase order for Black Friday was created retrospectively in December, this logic will ensure the marketing spend will be attributed to November)
(
select      *
            -- extracting the month of year
            ,case       when lower(payment_description) like '%january%' or lower(payment_description) like '%jan %' then '01'
                        when lower(payment_description) like '%february%' or lower(payment_description) like '%febraury%' or lower(payment_description) like '%feb%' then '02'
                        when lower(payment_description) like '%march%' or lower(payment_description) like '%mar %'then '03'
                        when lower(payment_description) like '%april%' or lower(payment_description) like '%apr %'then '04'
                        when lower(payment_description) like '%may %' then '05'
                        when lower(payment_description) like '%june%' or lower(payment_description) like '%jun %'then '06'
                        when lower(payment_description) like '%july%' or lower(payment_description) like '%jul %'then '07'
                        when lower(payment_description) like '%august%' or lower(payment_description) like '%aug %'then '08'
                        when lower(payment_description) like '%september%' or lower(payment_description) like '%sep%' or lower(payment_description) like '%sept %' then '09'
                        when lower(payment_description) like '%october%' or lower(payment_description) like '%oct%'then '10'
                        when lower(payment_description) like '%november%' or lower(payment_description) like '%nov%' or lower(payment_description) like '%black friday%' or lower(payment_description) like '%bf%' then '11'
                        when lower(payment_description) like '%december%' or lower(payment_description) like '%dec%'then '12'
                        else null end month_part

            -- extracting year
            ,case       when lower(payment_description) like '%2019%' or lower(payment_description) like '%19%' then '2019'
                        when lower(payment_description) like '%2020%' then '2020' --or lower(payment_description) like '%20%' 
                        when lower(payment_description) like '%2021%' or lower(payment_description) like '%21%' then '2021'
                        when lower(payment_description) like '%2022%' or lower(payment_description) like '%22%' then '2022'
                        when lower(payment_description) like '%2023%' or lower(payment_description) like '%23%' then '2023'
                        when lower(payment_description) like '%2024%' or lower(payment_description) like '%24%' then '2024'
                        when lower(payment_description) like '%2025%' or lower(payment_description) like '%25%' then '2025'
                        when lower(payment_description) like '%2026%' or lower(payment_description) like '%26%' then '2026'
                        else null end year_part 

            -- concatenating the above fields to create a year_month value 
            ,case     when month_part is not null and year_part is not null then concat(year_part,'-',month_part) 
                      when month_part is not null and year_part is null then concat(left(date_created,4),'-',month_part) 
                      else null end year_month_concat

            -- use date created when the above logic is null 
            ,case when year_month_concat is null then left(date_created,7) else null end year_month_created

            -- final year_month value
            ,case when year_month_concat is null then year_month_created else year_month_concat end year_month_inferred
                              
from         finance_data u
order by     month_part desc

)

--- JOINS ordered by level of accuracy from available data sources (i.e. prioritising 1-1 relationship over 1-many)

,description_string_join as 
-- using wildcards to join on purchase order descriptions where the string is LIKE an influencer name from a lookup list
(
select       l1.influencer_name, l1.influencer_id ,n.purchaseorder_id ,n.item_title ,n.payment_description ,n.finance_name ,n.amount_gbp ,n.department ,n.purchase_order_description 
            ,n.status ,n.date_created ,n.shipment_received ,n.amount_foreign ,n.currency ,n.country ,n.transaction_id ,n.year_month_inferred
from         month_year_concat n 
left join   (select influencer_name, influencer_id from influencer_marketing.influencer_database 
            where influencer_name is not null
            group by 1,2)  l1 
            on lower(n.payment_description) like lower(concat('%',l1.influencer_name,'%'))
)

,finance_number_join as 
-- for any PO's that could not be joined above, this CTE joins by finance number where finance number is unique to one talent only (i.e. removing duplication of talent agencies with multiple talent)
(
select       l2.influencer_name ,l2.influencer_id ,d.purchaseorder_id ,d.item_title ,d.payment_description ,d.finance_name ,d.amount_gbp ,d.department ,d.purchase_order_description 
            ,d.status ,d.date_created ,d.shipment_received ,d.amount_foreign ,d.currency ,d.country ,d.transaction_id ,d.year_month_inferred
from         description_string_join d
left join    (select influencer_name ,influencer_id ,finance_number 
             from influencer_marketing.influencer_database 
             where finance_number not in (select finance_number from influencer_marketing.influencer_database group by 1 having count(finance_number) > 1 )
             group by 1,2,3) l2
             on left(d.finance_name,11) = l2.finance_number
where        d.influencer_name is null 
)


,union_joined_data as -- merging both of the above datasets
(
select * from description_string_join where influencer_name is not null     union all     select * from finance_number_join
)

,final_cte as -- inserting a custom error mesage with call to action for when data appears in dashboard for Marketing Exec's
(
select      case when influencer_name is null then concat('PO MISSING FROM DATABASE - ',finance_name) else influencer_name end influencer_name 
            ,influencer_id ,finance_name ,purchaseorder_id ,year_month_inferred ,date_created ,item_title ,payment_description ,amount_gbp ,status ,department ,purchase_order_description ,transaction_id ,shipment_received ,amount_foreign ,currency
from        union_joined_data 
)

select * from final_cte 

)

;
