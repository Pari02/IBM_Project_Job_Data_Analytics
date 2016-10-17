# Author: Parikshita Tripathi
# Date: 09/26/2016

# installing and calling libraries
install.packages("ibmdbR")
library(ibmdbR)

# define and assign values to variables
dsn.name <- "****"
driver.name <- "*************"
db.name <- "****"
host.name <- "*************"
port <-"*****"
user.name <-"******"
pwd <- "*******"

# create a variable with connection parameters as values
con.text <- paste(dsn.name, 
                  ";DRIVER=",driver.name,
                  ";Database=",db.name,
                  ";Hostname=",host.name,
                  ";Port=",port,
                  ";PROTOCOL=TCPIP",
                  ";UID=", user.name,
                  ";PWD=",pwd, sep="")

# Connect to dashDB remote databse using a odbc Driver Connection string
con <- idaConnect(con.text)

# initialize the connection
idaInit(con)

# close connect
# idaClose(con)