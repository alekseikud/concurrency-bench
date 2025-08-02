DROP TABLE IF EXISTS test;
CREATE TABLE test(
	test_name 		 				TEXT NOT NULL,
	duration_ms 	 				REAL NOT NULL,
	concurrency 	 				INT NOT NULL,
	action_type 	 				TEXT NOT NULL,
	best_time_ms     				REAL ,        
 	worst_time_ms    				REAL ,   
	success_count 					INT NOT NULL,
	error_count 	 				INT NOT NULL,
	made_at  TIMESTAMPTZ DEFAULT NOW()
)