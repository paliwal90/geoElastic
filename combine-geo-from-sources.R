library(data.table)
library(jsonlite)

# function to aggregate data by source
aggregate_by_source <- function (source) {
  dir <- paste0("/home/paliwal/geo/poi/",source,"/")
  all.files <- list.files(path=dir)  
  numfiles <- length(all.files)  
  setwd(dir)
  if (source=="osm") { l <- lapply(all.files, fread, sep="|") }
  else {
    l <- lapply(all.files, fread, sep=",")
  }
  dt <- rbindlist( l )
  return(dt)
}

###################################
# Treat Open Street Map (OSM) data
###################################
osm <- aggregate_by_source ("osm")
colnames(osm)<-c("cat_id", "element_id","lat","lon","poi_name")
filter_types <- fread("/home/paliwal/geo/poi/filters_types_id.txt")
colnames(filter_types)<-c("cat","cat_id")
osm <- merge(x=osm,y=filter_types,by="cat_id", all.x=TRUE)
osm$source <- "osm"
osm$address <- ""
osm <- subset(osm,select=c(lon,lat,poi_name,address,source,cat))

###################################
# Treat PoiFactory data
###################################
poifactory <- aggregate_by_source ("poifactory")
colnames(poifactory)<-c("lon","lat","poi_name","address")
poifactory$source <- "poifactory"
poifactory$cat <- ""

###################################
# Treat PocketGps data
###################################
pocketgps <- aggregate_by_source ("pocketgps")
colnames(pocketgps)<-c("lon","lat","poi_name","address")
pocketgps$source <- "pocketgps"
pocketgps$cat <- ""

###################################
# Combine
###################################
df <- rbind (osm,poifactory,pocketgps)
df$location = paste0(df$lat,",",df$lon) #preferred for putting Mapping file type further on ElasticSearch
df<-subset(df,select=c(location,cat,poi_name,address,source))

print("Converting to json...")
x <- toJSON(df)
write(x,"/home/paliwal/geo/poi/geo.json")

print("Randomly selecting n locations...")
n=50000
write.table(sample(df$location,n),file="/home/paliwal/geo/poi/sampled_locations.csv",sep="\t",row.names = F,col.names = T,quote=F)


