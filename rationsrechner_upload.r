# Programma afbreken bij iedere foutmelding
options(warn = 2)

# Libraries
library(RODBC)
library(dplyr)
library(RPostgres)
library(svDialogs)
library(dotenv)

load_dot_env()

host = Sys.getenv('PG_HOST')
username = Sys.getenv('PG_USER')
password <- Sys.getenv('PG_PASSWORD')
database <- Sys.getenv('PG_DATENBANK')
schema_name <- Sys.getenv('PG_SCHEMA')

if(host == '' || username == '' || password == '' || database == '' || schema_name == ''){
  stop('Es fehlt .env file mit PG_HOST, PG_USER, PG_PASSWORD, PG_DATENBANK oder PG_SCHEMA')
}

# Verbinding parameters
access_bestand <- choose.files(caption = "Selektier Access Datenbank", multi = FALSE, filters = c("Access", "*.accdb;*.mdb"))
if(length(access_bestand) == 0) stop('Selektier Access Datenbank')

# Verbinding maken met Access
accdb <- odbcConnectAccess(access_bestand, DBMSencoding ='utf8')

# Lijst van alle Access-tabellen
tabellen <- sqlTables(accdb)
tabellen <- tabellen %>%
	filter(TABLE_TYPE == 'TABLE')
tabellen <- tabellen$TABLE_NAME

# Handige hulpfunctie
quer <- function(...) sqlQuery(accdb, paste0(...))

# PostgreSQL-verbinding maken
pgdb <- dbConnect(Postgres(), 
									host = host, 
									dbname = database, 
									user = username, password = password)

# Een voor een de tabellen uitlezen en wegschrijven
for(tabel in tabellen){
  tabel <- iconv(tabel, to = 'utf8')

	print(tabel)
	
	# Lijstje van alle kolommen die in de tabel zijn
	kolommen <- sqlColumns(accdb, `tabel`)
	kolommen <- kolommen %>% select(kolom = COLUMN_NAME, type = TYPE_NAME)
	kolommen <- kolommen %>% filter(!grepl('BLOB', type))	# BLOB betekent 'binary large object'. Zulke kolommen willen we overslaan. Waarschijnlijk ten overvloede; wel relevant voor Herde.
	kolommen$kolom[is.na(kolommen$kolom)] <- "NA"
	kolommen$kolom2 <- NA
	
	for(i in 1:nrow(kolommen)){
		if(kolommen$type[i] != 'BIT'){
			kolommen$kolom2[i] <- paste0('`', kolommen$kolom[i], '`')
		} else{
			kolommen$kolom2[i] <- paste0('cint(`', kolommen$kolom[i], '`)')
		}
	}
	

	# De tabel uitlezen en in een tijdelijke tabel opslaan. De tijdelijke tabel heet hier 'dat'.
	query <- paste(kolommen$kolom2, collapse = ',')
	query <- paste0('select ', query, ' from [', tabel, ']')
	query <- iconv(query, to = 'utf8')
	dat <- quer(query)
	colnames(dat) <- kolommen$kolom
	
	# Zorgen dat de datatypes van de kolommen kloppen. Met name: zorgen dat datums als datums worden herkend en gehele getallen zonder komma worden opgeslagen (bijvoorbeeld: 7 in plaats van 7.0)
	integers <- kolommen$kolom[kolommen$type %in% c('COUNTER', 'BIGINT','INTEGER', 'SMALLINT')]
	datums <- kolommen$kolom[kolommen$type == 'DATE']
	currencies <- kolommen$kolom[kolommen$type == 'CURRENCY']
	bits <- kolommen$kolom[kolommen$type == 'BIT']
	for(k in integers) dat[,k] <- as.integer(dat[,k])
	for(k in datums) dat[,k] <- as.Date(dat[,k])
	for(k in bits) dat[,k] <- as.logical(dat[,k])
	for(k in 1:ncol(dat)){
		if(is.character(dat[,k])){
			dat[,k] <- iconv(dat[,k], to = 'utf8')
		}
	}
	
	# Zorgen dat de kolomnamen alleen kleine letters hebben. Dat is handiger voor later.
	colnames(dat) <- tolower(colnames(dat))

	colnames(dat) <- iconv(colnames(dat), to = 'utf8')
	# tabel2 <- tolower(tabel)
	tabel2 <- tabel
	tabel2 <- paste0('"', tabel2, '"')
	tabel2 <- iconv(tabel2, to = "utf8")
	# De tijdelijke tabel wegschrijven naar de PostgreSQL-database
	dbWriteTable(pgdb, name = DBI::SQL(paste0(schema_name,".", tabel2)), 
							 value = dat, row.names = FALSE, overwrite = FALSE)
}

cat("\n\nFertig\n")
# Alle verbindingen afsluiten
odbcCloseAll()
dbDisconnect(pgdb)
