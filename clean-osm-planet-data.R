dt<-readLines("/home/paliwal/geo/poi/planet-latest.csv") # not use fread due to mismatch in rows in uncleaned planet file.

cut_row_key <- vector(mode="numeric", length=length(dt))
cut_row_value<-vector(mode="character", length=length(dt))

full_row_key<-vector(mode="numeric", length=length(dt))
full_row_value<-vector(mode="character", length=length(dt))

print("Clearning in progress...")

i_c=0
i_f=0
for (i in 1:length(dt)){
  #print(i)
  if(length(unlist(strsplit(dt[i],"\\|")))<=1){
    i_c <- i_c + 1
    cut_row_key[i_c]<-i-1
    cut_row_value[i_c]<-dt[i]
  }
  else{
    i_f <- i_f + 1
    full_row_key[i_f]<-i
    full_row_value[i_f]<-dt[i]
  }
}

full<-data.table(num=full_row_key[1:i_f],f_value=full_row_value[1:i_f])
cut<-data.table(num=cut_row_key[1:i_c],c_value=cut_row_value[1:i_c])

dt_merged <- merge(x=full,y=cut,by="num", all.x=TRUE)
dt_merged[is.na(dt_merged)] <- ""
dt_merged$value<-paste0(dt_merged$f_value,dt_merged$c_value,sep=" ")

write.table(dt_merged$value,file="/home/paliwal/geo/poi/osm/planet-cleaned.csv",row.names = F,col.names = F, quote = F)
