# Programma afbreken bij iedere foutmelding
options(warn = 2)

# Libraries
library(RODBC)
library(dplyr)
library(RPostgres)
library(svDialogs)
library(dotenv)
library(stringr)

load_dot_env()

host <- Sys.getenv('PG_HOST')
username <- Sys.getenv('PG_USER')
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
# accdb <- odbcConnectAccess(access_bestand, DBMSencoding ='utf8')
accdb <- odbcDriverConnect(paste0(
  "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", access_bestand))

# PostgreSQL-verbinding maken
pgdb <- dbConnect(Postgres(),
                  host = host,
                  dbname = database,
                  user = username, password = password)


# Lijst van alle Access-tabellen
access_tabellen <- sqlTables(accdb)
access_tabellen <- access_tabellen %>%
	filter(TABLE_TYPE == 'TABLE') |>
  select(table_name_old = TABLE_NAME)
access_tabellen <- access_tabellen |>
  mutate(table_name_new = gsub('ü', 'ue', table_name_old)) |>
  mutate(table_name_new = gsub('ä', 'ae', table_name_new)) |>
  mutate(table_name_new = gsub('ö', 'oe', table_name_new))

pg_tabellen <- dbGetQuery(pgdb, str_glue(
           "select table_name from information_schema.tables
                   where table_schema='{schema_name}'"))

vgl_access_pg <- pg_tabellen |>
  left_join(access_tabellen, keep = TRUE, by = c('table_name' = 'table_name_new'), suffix = c('_pg', '_ac'))

tabellen <- vgl_access_pg |>
  select(table_name_old, table_name_new) |>
  filter(!is.na(table_name_old))

# Handige hulpfunctie
quer <- function(...) sqlQuery(accdb, paste0(...))

for(l in 24:nrow(tabellen)){
  tabel <- tabellen$table_name_old[l]

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

	tabel_naar <- tabellen$table_name_new[l]
	tabel_naar <- paste0('"', tabel_naar, '"')
	tabel_naar <- iconv(tabel_naar, to = "utf8")
  id_kolommen_nieuw <- dbGetQuery(pgdb, str_glue("
    select *
    from information_schema.columns
    where table_schema = '{schema_name}'
    and table_name = '{tabel_naar}'
    and column_name = 'id'
    and column_default like 'nextval%'"))


  # Zorgen dat de kolomnamen alleen kleine letters hebben. Dat is handiger voor later.
	colnames(dat) <- tolower(colnames(dat))
	colnames(dat) <- iconv(colnames(dat), to = 'utf8')
	colnames(dat)[which(colnames(dat) == '€')] <- 'e'

	if(tabel == 'tbl Milmenge 1te Mahlzeit und Brix'){
	  colnames(dat)[which(colnames(dat) == 'id')] <- 'id_alt'
	}
	if(tabel == 'Rations Namen'){
	  colnames(dat)[which(colnames(dat) == 'datum "von"')] <- 'datum von'
	  colnames(dat)[which(colnames(dat) == 'datum "bis"')] <- 'datum bis'
	}

  if(nrow(id_kolommen_nieuw) > 0 && any(colnames(dat) == 'id')){
    stop()
  }

	# De tijdelijke tabel wegschrijven naar de PostgreSQL-database
	dbExecute(pgdb, str_glue('truncate table {schema_name}.{tabel_naar}'))
	dbWriteTable(pgdb, name = DBI::SQL(paste0(schema_name,".", tabel_naar)),
	  value = dat, row.names = FALSE, overwrite = FALSE, append = TRUE)
}

cat("\n\nFertig\n")
# Alle verbindingen afsluiten
odbcCloseAll()
dbDisconnect(pgdb)
