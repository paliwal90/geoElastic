library(data.table)
library(jsonlite)

### SPECIFIC POIFACTORY ADDED CATEGORY
dir <- paste0("/home/paliwal/geo/poi/poifactory/")

supermarket <- fread(paste0(dir,"Aldi.csv"),sep=",") #
supermarket <- rbind(supermarket,fread(paste0(dir,"Costco_USA_Canada.csv"),sep=","))
supermarket <-  rbind(supermarket,fread(paste0(dir,"Walmart_USA_Canada.csv"),sep=",")) 

colnames(supermarket)<-c("lon","lat","poi_name","address")
supermarket$source <- "poifactory"
supermarket$cat <- "shop_supermarket"

food <- fread(paste0(dir,"Tim Hortons_CANADA.csv"),sep=",") #
food <- rbind(food,fread(paste0(dir,"Burger 21.csv"),sep=","))
colnames(food)<-c("lon","lat","poi_name","address")
food$source <- "poifactory"
food$cat <- "amenity_fast_food"

df <- rbind(supermarket,food)
df$location = paste0(df$lat,",",df$lon) #preferred for putting Mapping file type further on ElasticSearch
df<-subset(df,select=c(location,cat,poi_name,address,source))

print("Converting to json...")
x <- toJSON(df)
write(x,"/home/paliwal/geo/poi/geo_specific_poifactory.json")
