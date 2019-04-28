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
