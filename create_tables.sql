DROP TABLE IF EXISTS test;
CREATE TABLE test(
	test_id 		 INT PRIMARY KEY,
	duration_ms 	 INT NOT NULL,
	concurrency 	 INT NOT NULL,
	action_type 	 TEXT NOT NULL,
	items_processed  INT NOT NULL,
	best_time_ms     REAL NOT NULL,        
 	worst_time_ms    REAL NOT NULL,   
	success_count 	 INT NOT NULL,
	error_count 	 INT NOT NULL,
	error_message 	 TEXT
)