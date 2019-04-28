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
