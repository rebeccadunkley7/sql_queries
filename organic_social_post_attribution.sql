/*
REGULAR EXPRESSIONS TO ATTRIBUTE ORGANIC SOCIAL PERFORMANCE TO INFLUENCERS

Scenario: A talent manager would like to understand the value of their influencer's have driven for their brand partner through organic social reposts.
Solution: This query seeks aggregate the organic social metrics where an influencer was posted, by using regular expressions to extract social handles & join to influencer datasets.

*/

with social_handles as 
-- generating a list of influencer social handles from a table that populates with influencer posts / database, along with a unique ID
(
select     lower(s.social_handle) lookup_name ,l.influencer_id 
from       influencer_marketing.content_posted s
left join  influencer_marketing.influencer_database l using (influencer_id2)
group by   1,2 
)

,string_extracts as 
-- extracting social handles from company posts where a substring beginning with '@' is located and extracted, repeated multiple times where more than one influencer / account is tagged 
(
SELECT      o.content_id ,o.provider_name ,o.CAPTION ,published_at
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)') not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)'), '@') else null end regex_string_1 
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,2) not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,2),'@') else null end regex_string_2
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,3) not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,3), '@') else null end regex_string_3
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,4) not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,4), '@') else null end regex_string_4
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,5) not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,5), '@') else null end regex_string_5
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,6) not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,6),'@') else null end regex_string_6            
            ,case when regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,7) not like '@companyname%' then regexp_replace(regexp_substr(lower(caption), '@([a-zA-Z0-9_.]+)',1,7),'@') else null end regex_string_7

from performance_marketing.organic_social_content o
where ("date" > '2023-12-31') and contains(caption, '@') and published_at is not null 
group by 1,2,3,4
)

,lookup_join as 
-- using the first CTE to join a unique ID to recognised influecer accounts (that we want to attribute)
(
select     content_id ,provider_name ,caption ,published_at
            ,regex_string_1 ,s.influencer_id influencer_id_1 ,regex_string_2 ,s2.influencer_id influencer_id_2 
            ,regex_string_3 ,s3.influencer_id influencer_id_3 ,regex_string_4 ,s4.influencer_id influencer_id_4 ,regex_string_5 ,s5.influencer_id influencer_id_5
            ,regex_string_6 ,s6.influencer_id influencer_id_6 ,regex_string_7 ,s7.influencer_id influencer_id_7
from       string_extracts x
left join  social_handles s on s.lookup_name = x.regex_string_1
left join  social_handles s2 on s2.lookup_name = x.regex_string_2
left join  social_handles s3 on s3.lookup_name = x.regex_string_3 
left join  social_handles s4 on s4.lookup_name = x.regex_string_4
left join  social_handles s5 on s5.lookup_name = x.regex_string_5
left join  social_handles s6 on s6.lookup_name = x.regex_string_6
left join  social_handles s7 on s7.lookup_name = x.regex_string_7
)

,union_ids as 
-- creating a tidy list that stacks social posts & attributed influencer, creating one line for each creator that was mentioned
(
select content_id ,provider_name ,caption ,published_at ,influencer_id_1 influencer_id
from lookup_join
where influencer_id_1 is not null

union all 

select content_id ,provider_name ,caption ,published_at ,influencer_id_2 influencer_id
from lookup_join
where influencer_id_2 is not null

union all 

select content_id ,provider_name ,caption ,published_at ,influencer_id_3 influencer_id
from lookup_join
where influencer_id_3 is not null

union all 

select content_id ,provider_name ,caption ,published_at ,influencer_id_4 influencer_id
from lookup_join
where influencer_id_4 is not null

union all 

select content_id ,provider_name ,caption ,published_at ,influencer_id_5 influencer_id
from lookup_join
where influencer_id_5 is not null

union all 

select content_id ,provider_name ,caption ,published_at ,influencer_id_6 influencer_id
from lookup_join
where influencer_id_6 is not null

union all

select content_id ,provider_name ,caption ,published_at ,influencer_id_7 influencer_id
from lookup_join
where influencer_id_7 is not null

)

-- merging the max post performance metrics to the unioned list of attributed posts & influencers
select u.* ,l.publisher_name ,p.max_lifetime_engagement ,p.max_lifetime_reach ,p.max_lifetime_engagement_rate ,p.max_video_views_lifetime
from union_ids u
left join (select publisher_name, influencer_id from influencer_marketing.influencer_database group by 1,2 ) l using(influencer_id)
left join (select content_id ,post_url ,max(lifetime_engagement) max_lifetime_engagement ,max(lifetime_reach) max_lifetime_reach 
                  ,max(lifetime_engagement_rate) max_lifetime_engagement_rate ,max(video_views_lifetime) max_video_views_lifetime
            from performance_marketing.organic_social_content where content_id in (select content_id from union_ids group by 1) group by 1,2) p  using(content_id)

;
