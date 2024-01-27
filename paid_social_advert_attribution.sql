/*
STRING MANIPULATION & JOINS TO ATTRIBUTE PAID SOCIAL ADS

Scenario: A talent manager would like to evaluate the commercial impact of their Influencer outside of last click affiliate link / discount code revenue.
Solution: This query seeks to generate a view of ad performance where the influencer was featured in campaign names. Incremental value would be determined by benchmarking vs BAU or other relevant campaigns.

*/

create table influencer_marketing.paid_social_campaigns as (

with influencer_list as 
--- creating a master lookup in the query with variations of ways influencer names could have been written in paid social campaign titles
(
  
--- creating a list of lower name, extracted straight from the database
select lower(influencer_name) name ,influencer_id 
from influencer_marketing.influencer_database group by 1,2 

union all

-- creating the same list, but removing spaces between words
select lower(replace(influencer_name,' ')) name ,influencer_id 
from influencer_marketing.influencer_database 
group by 1,2 

union all 

-- creating a list of social handles only
select s.social_handle name ,l.influencer_id 
from influencer_marketing.content_posted s
left join influencer_marketing.influencer_database l using (influencer_id2)
group by 1,2 
)

,psocial_campaign_names as
-- creating a look up of paid social campaign names
(
select LOWER(p.campaign) AS campaign, MAX(p.date) AS max_date, p.platform, p.account_name, p.campaign_id  
from performance_marketing.paid_social_campaigns p
where (date between '2022-04-01' and current_date() )
group by 1,3,4,5
)

,lookup_joins as
-- 
(
select       m.campaign ,m.max_date ,m.platform  ,m.social_campaign_id ,l.influencer_id  ,i.name
from         psocial_campaign_names m
inner join    (select * from influencer_list where length(name) > 4 group by 1,2) l ON lower(m.campaign) LIKE CONCAT('%', l.name, '%')
left join    (select name ,influencer_id from influencer_marketing.influencer_database) i using(influencer_id)
group by     1,2,3,4,5,6,7
)

-- pulling the ad performance into the query using the lookup lists defined in CTE above, taking an aggregated total of performance metrics  
select       l.publisher_name ,l.creator_number ,cal_date 
            ,count(distinct m.campaign) distinct_campaigns ,count(distinct m.territory) distinct_territory ,count(distinct m.channel) distinct_paid_channels
            ,sum(m.impressions) sum_impressions ,sum(m.spend) sum_spend ,sum(m.clicks) sum_clicks ,round(sum(m.ad_revenue)) sum_ad_revenue ,listagg(m.campaign) agg_campaign
from         performance_marketing.paid_social_campaigns m
left join    lookup_joins l on lower(l.campaign) = lower(m.campaign)
where        lower(m.campaign) in (select lower(campaign) from influencer_marketing.paid_social_campaigns group by 1)
group by     1,2,3 
order by     influencer_id
  
)
;
