DROP TABLE IF EXISTS test;
CREATE TABLE test(
	id 				 SERIAL,
	duration_ms 	 INT NOT NULL,
	concurrency 	 INT NOT NULL,
	action_type 	 TEXT NOT NULL,
	items_processed  INT NOT NULL,
	best_time_ms     REAL NOT NULL,   
  	median_time_ms   REAL NOT NULL,      
 	worst_time_ms    REAL NOT NULL,   
	success_count 	 INT NOT NULL,
	error_count 	 INT NOT NULL,
	error_message 	 TEXT
)