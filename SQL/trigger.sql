CREATE OR REPLACE FUNCTION test_type()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	IF NEW.test_name ~ 'threading_test' THEN
		NEW.test_type := 'multithreading';
	ELSIF NEW.test_name ~ 'processing_test' THEN
		NEW.test_type := 'multiprocessing' ;
	ELSIF NEW.test_name ~ 'async_test' THEN
		NEW.test_type := 'asyncio';
	ELSE
    	NEW.test_type := 'unknown';
	END IF;
	RETURN NEW;
END;
$$;


DROP TRIGGER IF EXISTS type_test ON test;

CREATE TRIGGER type_test
BEFORE INSERT ON test
FOR EACH ROW
EXECUTE FUNCTION test_type();