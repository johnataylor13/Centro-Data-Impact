#Centro Impact

%sparksql
#Joining all the data together
select *
into centro_impact
from centro_impressions as imp 
join imp.'Impressions ID' on centro_clicks.'Impression ID'
join imp.'Advertiser ID' on match_table_ads.'Advertiser ID' and imp.'Campaign_ID' on match_table_ads.'Campaign ID'
join imp.'Advertiser ID' on match_table_advertisers.'Adveriser ID'
join imp.'Browser/Platform ID' on match_table_browser.'Browser/Platform ID'
join imp.'Advertiser ID' on match_table_campaigns.'Advertiser ID' and imp.'Campaign ID' on match_table_campaigns.'Campaign ID'
join imp.'Ad ID' on match_table_creative_ad_assignments.'Ad ID'
join imp.'Advertiser ID' on match_table_creatives.'Advertiser ID' and imp.'Rendering ID' on match_table_creatives.'Rendering ID'
join imp.'Placement ID' on match_table_placement_cost.'Placement ID'
join imp.'Ad ID' on match_table_keyword_value.'Ad ID'
join imp.'Site ID' on match_table_placements.'Site ID' and imp.'Placement ID' on match_table_placements.'Placement ID'
join imp.'Site ID' on match_table_sites.'Site ID'

#Check which impressions within centro_impressions are completed or clicked (do they exist in the completed_impressions dataset)
select *, 
click = case when 'Landing Page URL ID' != '' then 1 else 0 end,
completed = case when 'Rich_Media_Event_ID' == 13 then 1 else 0 end,
analysis_flag = case when click == 1 and completed == 1 then 1 else 0 end,
'Placement Rate'/1000000000000 as placement_spend_USD
into centro_impact1
from centro_impact

#Windowing function will now calculate depth for every observation for those impressions that were either completed or clicked
SELECT * count(unique_id)-row_number(unique_id) OVER(partition by user_id order by 'Event Time') AS depth 
into centro_impact_completes
FROM centro_impact1
WHERE load_dt = '2017-08-27' AND analysis_flag == 1

#Creating Table of Incompletes
SELECT *  
into centro_impact_incompletes
FROM centro_impact1
WHERE load_dt = '2017-08-27' AND analysis_flag == 0

#Joining completes with incompletes
SELECT *
INTO centro_impact_final
FROM centro_impact_completes
JOIN centro_impact_completes.'Impression ID' ON centro_impact_incompletes.'Impression ID' 



%r
#Now the three variables we have are clicks, completions, and ad depth.  Need to find a way to standardize depth
impact_impressions1$standardized_depth <- as.numeric(cut2(impact_impresions1$depth, g=10))

#This is the part that will eventually just be done in Tableau.  For now though, this is how we are experimenting with the calculation
#Creating a summary table for Placement ID
impact_placementID <- impact_impressions1 %>%
  group_by('Placement ID') %>% #make this dynamic
  summarise(average_clicks = mean(click),
            average_completion = mean(completed),
            average_depth = mean(standardized_depth),
            average_spend = mean(placement_spend_USD)) %>%
  mutate(impact = XXXXXXX/average_spend)





