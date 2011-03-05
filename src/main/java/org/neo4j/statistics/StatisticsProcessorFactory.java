package org.neo4j.statistics;

import org.neo4j.graphdb.GraphDatabaseService;

import java.io.PrintStream;
import java.util.List;

public interface StatisticsProcessorFactory {
	StatisticsProcessor getProcessor( GraphDatabaseService graphDb, List<String> args, PrintStream out );

	String name();

	String argsHelp();
}
