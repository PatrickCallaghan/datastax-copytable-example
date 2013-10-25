Create a test keyspace to use

	create KEYSPACE test_copy WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
	
	