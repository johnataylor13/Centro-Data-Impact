#NFL Ad Impact Script - 8.27.17
######################################
#######CENTRO DATA PROCESSING#########
######################################
library(SparkR)
library(dplyr)

#######Combining Centro Data for Impression level########
#reading in the individual datasets - this needs to be done from the Spark style of dataframe
activity <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
impression <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
click <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_rich_media <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_custom_rich_media <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_activity_cat <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_activity_types <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_advertisers <- xSparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_browsers <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_campaigns <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_creative_ad_assignments <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_creatives <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_ads <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_keyword_values <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_placements <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_placement_costs <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))
match_table_sites <- SparkR::collect(read.df(sqlContext, "s3://gpg-data-primary/dma/centro_dcm/",source = "com.databricks.spark.csv", header="true", inferSchema = "true"))

#################################
########IMPACT IMPRESSIONS#######
#################################
#Building off of the impression dataset to create the complete impact dataframe
impact_impressions <- impression
impact_impressions$Event.Time <- as.numeric(impact_impressions$Event.Time)
impact_clicks$Event.Time <- as.numeric(impact_clicks$Event.Time)
impact_impressions$Date.Time <- as.numeric(substr(impact_impressions$Event.Time, 1,10))
impact_impressions$Date.Time <- as.POSIXct(impact_impressions$Date.Time, origin="1970-01-01", tz = "UTC")
impact_impressions$depth <- NULL
impact_impressions <- impact_impressions[order(desc(impact_impressions$Event.Time)),] #This ordering is done for when we calculate depth

#Tying impact_impressions to all match tables
#Tying impression to click
impact_impressions$click - ifelse(impact_impressions$"Impression ID" %in% click$"Impression ID", 1, 0)
#Tying impression to match_table_ads
impact_impressions <- merge(impact_impressions, match_table_ads, by=c("Adveriser ID", "Campaign ID", "Ad ID"))
#Tying impression to match_table_advertisers
impact_impressions <- merge(impact_impressions, match_table_advertisers, by=c("Adveriser ID"))
#Tying impression to match_table_browsers
impact_impressions <- merge(impact_impressions, match_table_browsers, by=c("Browser/Platform ID"))
#Tying impression to match_table_campaigns
impact_impressions <- merge(impact_impressions, match_table_campaigns, by=c("Advertiser ID", "Campaign ID"))
#Tyimg impression to match_table_creative_ad_assignments
impact_impressions <- merge(impact_impressions, match_table_creative_ad_assignments, by=c("Ad ID"))
#Tyimg impression to match_table_creatives
impact_impressions <- merge(impact_impressions, match_table_creatives, by=c("Advertiser ID", "Rendering ID"))
#Tying impression to match_table_placement_cost
impact_impressions <- merge(impact_impressions, match_table_placement_cost, by=c("Placement ID"))
#Tying impression to match_table_keyword_value
impact_impressions <- merge(impact_impressions, match_table_keyword_value, by=c("Ad ID"))
#Tying impression to match_table_placements
impact_impressions <- merge(impact_impressions, match_table_placements, by=c("Site ID", "Placement ID"))
#Tying impression to match_table_sites after joining on table_placements
impact_impressions <- merge(impact_impressions, match_table_sites, by=c("Site ID"))
#Deleting Columns with Identical Data in the impact_impressions dataset
impact_impressions[sapply(impact_impressions, function(x) length(unique(x))>1)]

#################################
##########IMPACT ACTIONS######### Probably not important we go over this
#################################
#Building off of the impression dataset to create the complete impact dataframe
impact_actions <- actions
impact_actions$Event.Time <- as.numeric(impact_actions$Event.Time)
impact_clicks$Event.Time <- as.numeric(impact_clicks$Event.Time)
impact_actions$Date.Time <- as.numeric(substr(impact_actions$Event.Time, 1,10))
impact_actions$Date.Time <- as.POSIXct(impact_actions$Date.Time, origin="1970-01-01", tz = "UTC")
impact_actions$depth <- NULL
impact_actions <- impact_actions[order(desc(impact_actions$Event.Time)),]
#Tying impression to match_table_activity_cat
impact_actions <- merge(impact_actions, match_table_activity_cat, by=c("Activity ID"))
#Tying impression to match_table_activity_types
impact_actions <- merge(impact_actions, match_table_activity_types, by=c("Activity Group ID"))
#Tying impression to match_table_ads
impact_actions <- merge(impact_actions, match_table_ads, by=c("Adveriser ID", "Campaign ID", "Ad ID"))
#Tying impression to match_table_advertisers
impact_actions <- merge(impact_actions, match_table_advertisers, by=c("Adveriser ID"))
#Tying impression to match_table_browsers
impact_actions <- merge(impact_actions, match_table_browsers, by=c("Browser/Platform ID"))
#Tying impression to match_table_campaigns
impact_actions <- merge(impact_actions, match_table_campaigns, by=c("Advertiser ID", "Campaign ID"))
#Tyimg impression to match_table_creative_ad_assignments
impact_actions <- merge(impact_actions, match_table_creative_ad_assignments, by=c("Ad ID"))
#Tyimg impression to match_table_creatives
impact_actions <- merge(impact_actions, match_table_creatives, by=c("Advertiser ID", "Rendering ID"))
#Tying impression to match_table_placement_cost
impact_actions <- merge(impact_actions, match_table_placement_cost, by=c("Placement ID"))
#Tying impression to match_table_keyword_value
impact_actions <- merge(impact_actions, match_table_keyword_value, by=c("Ad ID"))
#Tying impression to match_table_placements
impact_actions <- merge(impact_actions, match_table_placements, by=c("Site ID", "Placement ID"))

#Tying impression to match_table_sites after joining on table_placements
impact_actions <- merge(impact_actions, match_table_sites, by=c("Site ID"))

#Deleting Columns with Identical Data in the impact_actions dataset
impact_actions[sapply(impact_actions, function(x) length(unique(x))>1)]



#################################
####CREATING THE IMPACT SCORE####
#################################


#Now we need to calculate Average Completion using the Rich Media Lookup Table
#To do this, we just need to check if the impression ID fromt he impression table is in the Rich Media Full Table with the corresponding Rich Media EventID of '13'
#Need to first create a rich media table that only shows completed impressions
completed_impressions <- impact_rich_media %>%
  filter('Rich_Media_Event_ID' == 13)
impact_impressions$completed <- ifelse(impact_impressions$"Impression ID" %in% completed_impressions$'Impression ID', 1, 0)

#Calculating the depth of each ad along a user's path for ONLY those ads which were completed
unique_users <- as.list(unique(impact_impressions$User.ID))
impact_impressions <- impact_impressions[order(desc(impact_impressions$Event.Time)),]
impact_impressions$analysis_flag <- ifelse( impact_impressions$'User ID' != 0 & impact_impressions$completed == 1 | impact_impressions$click == 1, 1, 0)

#WINDOWING QUERY TO HANDLE LOOP/DATA PARTITION WITH DISTRIBUTED COMPUTING
%sparkSQL
SELECT * count(unique_id)-row_number(unique_id) OVER(partition by user_id order by 'Event Time') AS depth 
FROM impact_impressions1 
WHERE load_dt = '2017-08-27' AND analysis_flag == 1


%r
#Now need to bring back in those impressions that were NOT completed... they should be dinged for that
incomplete_impressions <- impact_impressions %>% 
  filter(completed == 0)
impact_impressions1 <- rbind(impact_impressions, incomplete_impressions)

#Now the three variables we have are clicks, completions, and ad depth.  Need to find a way to standardize depth
impact_impressions1$standardized_depth <- as.numeric(cut2(impact_impresions1$depth, g=10))

#Now we need to create a monetary variable to use as the denomenator in our per dollar impact calculation instead of billionths
impact_impressions1$placement_spend_USD <- impact_impressions1$'Placement Rate'/1000000000000



#This is the part that will eventually just be done in Tableau.  For now though, this is how we are experimenting with the calculation
#Creating a summary table for Placement ID
impact_placementID <- impact_impressions1 %>%
  group_by('Placement ID') %>% #make this dynamic
  summarise(average_clicks = mean(click),
            average_completion = mean(completed),
            average_depth = mean(standardized_depth),
            average_spend = mean(placement_spend_USD)) %>%
  mutate(impact = XXXXXXX/average_spend)








