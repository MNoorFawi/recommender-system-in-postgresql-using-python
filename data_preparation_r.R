suppressMessages(library(RPostgreSQL))
suppressMessages(library(DBI))
## connecting to the database with the credentials
pw <- "password"
drv <- dbDriver("PostgreSQL")
conn <- dbConnect(drv, dbname = "dvd_rental", 
                  host = "localhost", port = 5432,
                  password = pw, user = "postgres")

## query some tables
dbListTables(conn)

query <- 
  "SELECT customer.customer_id, customer.first_name, customer.last_name, 
inventory.inventory_id, film.film_id, film.title FROM customer  
FULL JOIN rental ON customer.customer_id = rental.customer_id
FULL JOIN inventory ON rental.inventory_id = inventory.inventory_id
FULL JOIN film ON inventory.film_id = film.film_id;"

customer_to_film <- dbGetQuery(conn, query)
head(customer_to_film)

library(dplyr)
movie_data <- customer_to_film %>% mutate(customer = paste(first_name, last_name)) %>%
  select(customer, movie = title)

head(movie_data)

set.seed(1311)
movie_sample <- sample(unique(movie_data$movie), 100)
movie_data_sampled <- movie_data %>% filter(movie %in% movie_sample) 

## one hot encode movies to 0 or 1 with customers
vars <- colnames(movie_data_sampled)

cat_vars <- vars[sapply(movie_data_sampled[, vars], class) %in%
                   c("factor", "character", "logical")]
cat_vars <- cat_vars[-1] 
movie_data_encoded <- data.frame(customer = unique(movie_data_sampled$customer))
for (i in cat_vars) {
  dict <- unique(movie_data_sampled[, i])
  for (key in dict) {
    movie_data_sampled[[paste0(key)]] <- as.integer(1.0 * (movie_data_sampled[, i] == key))
  }
}
# to remove the original categorical variables
movie_data_sampled[, which(colnames(movie_data_sampled) %in% cat_vars)] <- NULL
movie_data_sampled <- unique(movie_data_sampled)
head(movie_data_sampled)


movie_encoded <-
  aggregate(movie_data_sampled[, -1],
            by = list(customer = movie_data_sampled[, 1]),
            FUN = sum)

dim(movie_encoded)
write.csv(movie_encoded, "movies_data.csv", row.names = FALSE)
dbWriteTable(conn, "movies_rental", movie_encoded, row.names = 0)







