package org.neo4j.statistics.processors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.statistics.StatisticsProcessor;
import org.neo4j.statistics.StatisticsProcessorFactory;

import java.io.PrintStream;
import java.util.List;

public class PropertyTypeStatsFactory implements StatisticsProcessorFactory
{
    private static final long DEFAULT_CHUNK_SIZE = 5;

    @Override
    public StatisticsProcessor getProcessor( GraphDatabaseService graphDb, List<String> args, PrintStream out )
    {
        return new PropertyTypeStats( graphDb, out, getChunkSize( args ));
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

    private long getChunkSize( List<String> args )
    {
        if ( args.size() < 1 )
        {
            return DEFAULT_CHUNK_SIZE;
        }
        return Long.valueOf( args.get( 0 ) );
    }
}
