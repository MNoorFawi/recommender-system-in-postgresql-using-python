import psycopg2
import pandas as pd

psql = psycopg2.connect(host = "localhost", database = "dvd_rental",
                      user = "postgres", password = "psqldbase")
cursor = psql.cursor()
pd.read_sql("SELECT * FROM language;", psql)

sql = "SELECT * FROM movies_rental;"
movie_data = pd.read_sql(sql, psql)
movie_data.iloc[:, 0:4].head()

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

def hash_fun(df, list_of_columns):
    return df.iloc[:, list_of_columns].apply(lambda x: "".join(x.astype(str)), axis = 1)

hash_fun(movie_data[0:4], [1, 2, 3, 4])

compressed_movie["bucket1"] = hash_fun(movie_data, [1, 15, 23, 67, 89])
compressed_movie["bucket2"] = hash_fun(movie_data, [7, 12, 29, 44, 96])
compressed_movie["bucket3"] = hash_fun(movie_data, [33, 11, 3, 52, 74])

compressed_movie.head
compressed_movie[0:5]

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:psqldbase@localhost:5432/dvd_rental')
compressed_movie.to_sql("compressed_movies_rental", engine)



def create_index(column, cursor):
    sql = "CREATE INDEX %s ON compressed_movies_rental (%s);" % (column, column)
    cursor.execute(sql)
 
create_index("bucket1", cursor)
create_index("bucket2", cursor)
create_index("bucket3", cursor) 
psql.commit()

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

#example = ['{0:025b}'.format(255)]
bin_numbers = [b"11101111", b"00000100", b"11011111", b"11111111" ,
               b"11111111", b"10001001", b"11011111", b"11111111"]
example = [int(i, 2) for i in bin_numbers]
example


example_query = """SELECT hamming_distance%(example)s;"""
#ex = """SELECT HAMMINGDISTANCE%s;""" %(tuple(example))
#pd.read_sql(sql, psql) 
#cursor.execute(ex, {
#    "example" : tuple(example)
#})

sql = cursor.mogrify(example_query, {
        "example" : tuple(example)
    })

pd.read_sql(sql, psql) 


#'{0:025b}'.format(255)
#format(255, "025b")
#psql.commit()

customer = "Andrea Henderson"
sql = "SELECT * FROM compressed_movies_rental WHERE customer = '%s'" % customer
customer_data = pd.read_sql(sql, psql)
customer_data
sql = """ 
SELECT customer, hamming_distance(bit1, bit2, bit3, bit4, %s,%s,%s,%s) AS distance
FROM compressed_movies_rental WHERE bucket1 = '%s' OR bucket2 ='%s'
OR bucket3 = '%s' ORDER BY distance LIMIT 6;
""" % (customer_data.bit1[0], customer_data.bit2[0],
    customer_data.bit3[0], customer_data.bit4[0], customer_data.bucket1[0],
    customer_data.bucket2[0], customer_data.bucket3[0])
    
shortlist = pd.read_sql(sql, psql) 

query = "SELECT * FROM movies_rental WHERE customer IN %(customers)s" 
sql = cursor.mogrify(query, {
        "customers" : tuple(shortlist["customer"])
    })

neighbors = pd.read_sql(sql, psql)
rec = neighbors.T
col_number = neighbors[neighbors["customer"] == customer].index.item()
rec2 = rec.loc[rec[col_number] == 0]
watched_movies = rec2.apply(lambda x: any(x == 1), axis = 1)
rec3 = rec2[watched_movies]
list(rec3.index)


