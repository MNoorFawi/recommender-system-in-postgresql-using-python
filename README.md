Recommender in PostgreSQL Using Python
================

## Building a recommendation engine in postgreSQL using python

###### N.B. The database is built in [this post](https://github.com/MNoorFawi/neo4j-and-postgresql-with-R) and refer to the data\_preparation\_r.R script to have everything ready in the database.

Import libraries and connect to the database

``` python
import psycopg2
import pandas as pd

psql = psycopg2.connect(host = "localhost", database = "dvd_rental",
                      user = "postgres", password = "password")
cursor = psql.cursor()
## fetch some data to confirm connection
pd.read_sql("SELECT * FROM language;", psql)

#    language_id                  name         last_update
# 0            1  English              2006-02-15 10:02:19
# 1            2  Italian              2006-02-15 10:02:19
# 2            3  Japanese             2006-02-15 10:02:19
# 3            4  Mandarin             2006-02-15 10:02:19
# 4            5  French               2006-02-15 10:02:19
# 5            6  German               2006-02-15 10:02:19
```

Get the table we’re interested in

``` python
sql = "SELECT * FROM movies_rental;"
movie_data = pd.read_sql(sql, psql)
movie_data.iloc[:, 0:4].head()

#        customer  Idols Snatchers  Boogie Amelie  Scissorhands Slums
# 0   Aaron Selby                0              0                   0
# 1    Adam Gooch                0              0                   0
# 2  Adrian Clary                0              1                   0
# 3  Agnes Bishop                1              0                   0
# 4     Alan Kahn                0              0                   0
```

Now we will compress the data into only 4 columns; we will concatenate
the values of 25 movies together and they will look something like this
“10111”, then we will change this value to its unbinary equivalent
i.e. 23

We will define a function that does this all and define a new dataframe
to contain these 4 new columns

``` python
def unbinarize(df, start, end):
    ex = df.iloc[:, start:end].apply(lambda x: "".join(x.astype(str)), axis = 1)
    return [int(i, 2) for i in ex]

compressed_movie = pd.DataFrame()
compressed_movie["customer"] = movie_data["customer"]
compressed_movie["bit1"] = unbinarize(movie_data, 1, 26)
compressed_movie["bit2"] = unbinarize(movie_data, 26, 51)
compressed_movie["bit3"] = unbinarize(movie_data, 51, 76)
compressed_movie["bit4"] = unbinarize(movie_data, 76, 101)

compressed_movie[0:5]

#        customer      bit1     bit2     bit3      bit4
# 0   Aaron Selby         0        0     8192  16777216
# 1    Adam Gooch         0  2129920     8193         0
# 2  Adrian Clary   8388608      128        0  16777216
# 3  Agnes Bishop  16777216        0      512  16777216
# 4     Alan Kahn    557056        0  2097216         0
```

Now we will define a hash function to form 3 buckets to insert similar
customers together. These 3 buckets will contain patterns of rentals for
some random movies we chose.

``` python
def hash_fun(df, list_of_columns):
 return df.iloc[:, list_of_columns].apply(lambda x: "".join(x.astype(str)), axis = 1)

hash_fun(movie_data[0:4], [1, 2, 3, 4])

# 0    0000
# 1    0000
# 2    0100
# 3    1000

compressed_movie["bucket1"] = hash_fun(movie_data, [1, 15, 23, 67, 89])
compressed_movie["bucket2"] = hash_fun(movie_data, [7, 12, 29, 44, 96])
compressed_movie["bucket3"] = hash_fun(movie_data, [33, 11, 3, 52, 74])

compressed_movie[0:5]

#        customer      bit1     bit2     bit3      bit4 bucket1 bucket2 bucket3
# 0   Aaron Selby         0        0     8192  16777216   00000   00000   00000
# 1    Adam Gooch         0  2129920     8193         0   00000   00100   00000
# 2  Adrian Clary   8388608      128        0  16777216   00000   00000   00000
# 3  Agnes Bishop  16777216        0      512  16777216   10000   00000   00000
# 4     Alan Kahn    557056        0  2097216         0   00000   00000   00000
```

Now, we have our compressed movie data, let’s write it to the databse. I
prefer to use **sqlalchemy** for this as it is straightforward.

``` python
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:password@localhost:5432/dvd_rental')
compressed_movie.to_sql("compressed_movies_rental", engine)
```

We then create indices on the buckets to make retrieval faster

``` python
def create_index(column, cursor):
 sql = "CREATE INDEX %s ON compressed_movies_rental (%s);" % (column, column)
 cursor.execute(sql)
 
create_index("bucket1", cursor)
create_index("bucket2", cursor)
create_index("bucket3", cursor) 
psql.commit()
```

We will now define our similarity measurement. We will use **Hamming
Distance**. We will define two functions here, one (bits\_count) that
takes a number (mainly an output from bitwise xor), converts it to its
binary form and counts how many 1s in its binary form. This way we will
get a number describing in how many places the two numbers or customers
differ. As we have 4 bits columns, 25 movies each, we will need a
function that sums all the bits counts to give us a whole distance
number i.e. (hamming\_distance)
function.

###### We will define two functions for each because when we try, python throws integer type and when we will use it on the data it will use bigint type so we will define two of them to have them accepting both types.

``` python
bits_count = """
CREATE OR REPLACE FUNCTION bits_count(value bigint) RETURNS integer AS $$
DECLARE i integer;
        c integer;
        bits BIT(25);
BEGIN
    c := 0;
    bits := value::BIT(25);
    FOR i IN 1..LENGTH(bits) LOOP
        IF substring(bits, i, 1) = B'1' THEN
            c := c + 1;
        END IF;
    END LOOP;
    RETURN c;
END; 
$$  LANGUAGE plpgsql;

-- another one to accept only integer (coming from python calls mainly)

CREATE OR REPLACE FUNCTION bits_count(value integer) RETURNS integer AS $$
DECLARE i integer;
        c integer;
        bits BIT(25);
BEGIN
    c := 0;
    bits := value::BIT(25);
    FOR i IN 1..LENGTH(bits) LOOP
        IF substring(bits, i, 1) = B'1' THEN
            c := c + 1;
        END IF;
    END LOOP;
    RETURN c;
END; 
$$  LANGUAGE plpgsql;
"""
cursor.execute(bits_count)

ham_dist = """
CREATE OR REPLACE FUNCTION hamming_distance(
 A0 bigint, A1 bigint, A2 bigint, A3 bigint,
 B0 bigint, B1 bigint, B2 bigint, B3 bigint
)
RETURNS integer AS $$
BEGIN
RETURN 
    bits_count(A0 # B0) + 
    bits_count(A1 # B1) +
    bits_count(A2 # B2) + 
    bits_count(A3 # B3);
END; 
$$ LANGUAGE plpgsql;

-- another one to accept only integer (coming from python calls mainly)

CREATE OR REPLACE FUNCTION hamming_distance(
 A0 integer, A1 integer, A2 integer, A3 integer,
 B0 integer, B1 integer, B2 integer, B3 integer
)
RETURNS integer AS $$
BEGIN
RETURN 
    bits_count(A0 # B0) + 
    bits_count(A1 # B1) +
    bits_count(A2 # B2) + 
    bits_count(A3 # B3);
END; 
$$ LANGUAGE plpgsql;
"""
cursor.execute(ham_dist)
psql.commit()

## Try the functions
bin_numbers = [b"11101111", b"00000100", b"11011111", b"11111111" ,
               b"11111111", b"10001001", b"11011111", b"11111111"] # differ in 5 places 
example = [int(i, 2) for i in bin_numbers]
example

# [239, 4, 223, 255, 255, 137, 223, 255]

example_query = """SELECT hamming_distance%(example)s;"""

sql = cursor.mogrify(example_query, {
        "example" : tuple(example)
    })

pd.read_sql(sql, psql) # the result should be 5

#    hamming_distance
# 0                 5
```

Everything is working fine. Now we will start to recommend some data to
a customer.

``` python
customer = "Andrea Henderson"
sql = "SELECT * FROM compressed_movies_rental WHERE customer = '%s'" % customer
customer_data = pd.read_sql(sql, psql)
customer_data

#    index          customer  bit1   bit2  bit3     bit4 bucket1 bucket2 bucket3
# 0     22  Andrea Henderson     0  16384  1024  1048576   00000   00000   00000
```

Then we will measure the distance between our customer and the rest of
customers to get his/her nearest neighbours who have a similar taste.

``` python
sql = """ 
SELECT customer, hamming_distance(bit1, bit2, bit3, bit4, %s,%s,%s,%s) AS distance
FROM compressed_movies_rental WHERE bucket1 = '%s' OR bucket2 ='%s'
OR bucket3 = '%s' ORDER BY distance LIMIT 6;
""" % (customer_data.bit1[0], customer_data.bit2[0],
    customer_data.bit3[0], customer_data.bit4[0], customer_data.bucket1[0],
    customer_data.bucket2[0], customer_data.bucket3[0])
    
shortlist = pd.read_sql(sql, psql) 
shortlist

#            customer  distance
# 0  Andrea Henderson         0
# 1       April Burns         2
# 2       Rick Mattox         2
# 3       Floyd Gandy         3
# 4      Charlie Bess         3
# 5      Bob Pfeiffer         3
```

After knowing the neighbors of our customer, we will now filter our
movies table to recommend movies to our customer that he/she hasn’t
watched yet and their neighbors watched.

``` python
query = "SELECT * FROM movies_rental WHERE customer IN %(customers)s" 
sql = cursor.mogrify(query, {
        "customers" : tuple(shortlist["customer"])
    })

neighbors = pd.read_sql(sql, psql) # neighbors movies
rec = neighbors.T
col_number = neighbors[neighbors["customer"] == customer].index.item()
rec2 = rec.loc[rec[col_number] == 0] # drop movies that our customer has watched i.e. "!= 1"
# indices of movies at least one of the neighbors watched
watched_movies = rec2.apply(lambda x: any(x == 1), axis = 1) 
rec3 = rec2[watched_movies] # filter by watched movies only
rec3

#                       0  1  2  3  4  5
# Sweethearts Suspects  0  0  0  0  1  0
# Honey Ties            0  0  0  1  0  0
# Calendar Gunfight     0  0  1  0  0  0

list(rec3.index) # recommended movies

# ['Sweethearts Suspects', 'Honey Ties', 'Calendar Gunfight']
```

