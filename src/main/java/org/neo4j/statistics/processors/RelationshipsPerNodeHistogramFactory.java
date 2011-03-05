package org.neo4j.statistics.processors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.statistics.StatisticsProcessor;
import org.neo4j.statistics.StatisticsProcessorFactory;

import java.io.PrintStream;
import java.util.List;

public class RelationshipsPerNodeHistogramFactory implements StatisticsProcessorFactory
{
    private static final long DEFAULT_CHUNK_SIZE = 5;

    public StatisticsProcessor getProcessor( GraphDatabaseService graphDb, List<String> args, PrintStream out )
    {
        return new RelationshipsPerNodeHistogram( graphDb, out, getChunkSize( args ) );
    }

    public String name()
    {
        return "histo";
    }

    public String argsHelp()
    {
        return "[rel_chunk_size=5] - Prints histogram for relationships per node";
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
