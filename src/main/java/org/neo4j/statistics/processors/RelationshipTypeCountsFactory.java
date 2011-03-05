package org.neo4j.statistics.processors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.statistics.StatisticsProcessor;
import org.neo4j.statistics.StatisticsProcessorFactory;

import java.io.PrintStream;
import java.util.List;

public class RelationshipTypeCountsFactory implements StatisticsProcessorFactory
{
    public StatisticsProcessor getProcessor( GraphDatabaseService graphDb, List<String> args, PrintStream out )
    {
        return new RelationshipTypeCounts( graphDb, out );
    }

    public String name()
    {
        return "reltypes";
    }

    public String argsHelp()
    {
        return "- Aggregates information about relationship types";
    }
}
