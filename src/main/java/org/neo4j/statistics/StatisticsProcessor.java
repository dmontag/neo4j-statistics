package org.neo4j.statistics;

import org.neo4j.graphdb.GraphDatabaseService;

import java.io.PrintStream;

public interface StatisticsProcessor {
	void process();

	void reportProgress();

    void abort();
}
