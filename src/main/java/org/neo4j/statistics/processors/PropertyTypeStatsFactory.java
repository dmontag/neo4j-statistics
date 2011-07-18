package org.neo4j.statistics.processors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.statistics.StatisticsProcessor;
import org.neo4j.statistics.StatisticsProcessorFactory;

import java.io.PrintStream;
import java.util.List;

public class PropertyTypeStatsFactory implements StatisticsProcessorFactory
{
    @Override
    public StatisticsProcessor getProcessor( GraphDatabaseService graphDb, List<String> args, PrintStream out )
    {
        return new PropertyTypeStats( graphDb, out );
    }

    @Override
    public String name()
    {
        return "propstats";
    }

    @Override
    public String argsHelp()
    {
        return " - Print stats about properties";
    }
}
