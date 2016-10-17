# get database table list in a variable
table_list <- sqlTables(con, tableType = "TABLE", schema = "DASH015183")

# get table names in a list
table_names <- table_list$TABLE_NAME

# generate dataframes from the tables in the database
for (i in 1:length(table_names)){
  assign(table_names[i], idaQuery(paste("SELECT * FROM", table_names[i]), as.is = F))
}

# update column names 
colnames(SKILLSBYOCCUPATION) <- c("DEMANDPERCENTAGE", "SKILLID", "SKILLNAME", "SKILLRANK", "SKILLTYPE", "SkillOCCID")
colnames(SKILLCLUSTERSSBYOCCUPATIONSKILL) <- c("SKILLCLUSTERID", "SKILLCLUSTERNAME", "SKILLID", "SKILLCLUS_OCCID")
#colnames(stateareas) <- c('AREANAME', 'AREASORTORDER', 'ID', 'STATEAREANAME', 'STATENAME', 'STATESORTORDER')

# merge the skill data
skillData <- merge(SKILLSBYOCCUPATION, SKILLCLUSTERSSBYOCCUPATIONSKILL, 'SKILLID')

# verifying the variables
subCols <- names(OCCUPATIONSDATA) %in% c('cacareerPathsFrom', 'cacareerPathsTo', 'degrees', 'employers', 'id', 'occupationtitles', 'relatedOccupations')
OccDataNew <- OCCUPATIONSDATA[subCols]

# merge occupation data
OccupationMarketData <- merge(OCCUPATIONSBYSTATE, OccDataNew, 'id')

#length(grep(OccupationMarketData$areaId[1], stateareas$AREAID[4]))

# split the state area id 
stateareas['areaId'] <- (data.frame(do.call(rbind, strsplit(as.character(stateareas$ID), '/')))[3])

# merge state name with occupatiopationMarketData
OccupationMarket <- merge(OccupationMarketData, stateareas[c('AREANAME', 'areaId', 'STATEAREANAME', 'STATENAME')], 'areaId')

# saving data to tables
idaSave(con, OccupationMarket, tblName = "OCCUPATIONMARKET", conType = "odbc")
idaSave(con, skillData, tblName = "SKILLDATA", conType = "odbc")
