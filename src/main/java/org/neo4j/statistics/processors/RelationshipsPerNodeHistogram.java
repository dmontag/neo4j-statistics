package org.neo4j.statistics.processors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.statistics.Chunk;
import org.neo4j.statistics.Histogram;
import org.neo4j.statistics.StatisticsProcessor;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class RelationshipsPerNodeHistogram implements StatisticsProcessor
{
    private PrintStream out;
    private GraphDatabaseService graphDb;
    private volatile boolean shouldAbort;

    private Histogram<Long> histogram;

    public RelationshipsPerNodeHistogram( GraphDatabaseService graphDb, PrintStream out, long chunkSize )
    {
        this.graphDb = graphDb;
        this.out = out;
        histogram = new Histogram<Long>( chunkSize );
    }

    public void run()
    {
        for ( Node node : graphDb.getAllNodes() )
        {
            if ( shouldAbort ) return;
            int count = IteratorUtil.count( node.getRelationships() );
            histogram.record( node.getId(), count );
        }
    }


    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();

        result.append( histogram.toString( "Nodes", "Rels" ) );

        return result.toString();
    }

    public void writeRows( StringBuilder result, SortedMap<Long, Chunk<Long>> sortedResults )
    {
        histogram.writeRows( result, sortedResults );
    }

    public void writeRow( StringBuilder result, int rank, long count, long chunkKey, List<Long> samples, long aggregate, long weight, long aggregateWeight )
    {
        histogram.writeRow( result, rank, count, chunkKey, samples, aggregate, weight, aggregateWeight );
    }

    public Map<Long, Chunk<Long>> getCounts()
    {
        return histogram.getChunks();
    }

    public long getNodeCount()
    {
        return histogram.getTotalSamples();
    }

    public long getRelCount()
    {
        return histogram.getTotalCounts() / 2;
    }

    public void process()
    {
        run();
    }

    public void reportProgress()
    {
        out.print( this );
    }

    public void abort()
    {
        shouldAbort = true;
    }

//    public static void main( String[] args ) throws IOException
//    {
//        String path = args[0];
//        long chunkSize = Long.valueOf( args[1] );
//        String initDataFile = null;
//        if ( args.length > 2 )
//        {
//            initDataFile = args[2];
//        }
//        File configFile = new File( new File( path ), "neo4j.properties" );
//        Map<String, String> config = configFile.exists()
//            ? EmbeddedGraphDatabase.loadConfigurations( configFile.getAbsolutePath() )
//            : new HashMap<String, String>();
//        GraphDatabaseService graphDb = new EmbeddedReadOnlyGraphDatabase( path, config );
//        RelationshipsPerNodeHistogram result;
//        try
//        {
//            RelationshipsPerNodeHistogram histogram = new RelationshipsPerNodeHistogram( chunkSize );
//            if ( initDataFile != null )
//            {
//                histogram.init( new File( initDataFile ) );
//            }
//            result = histogram.run();
//        }
//        finally
//        {
//            graphDb.shutdown();
//        }
//        System.out.println( result );
//    }

//    private void init( File file ) throws IOException
//    {
//        BufferedReader reader = new BufferedReader( new FileReader( file ) );
//        for ( String line = reader.readLine(); line != null; line = reader.readLine() )
//        {
//            String[] parts = line.trim().split( "\\s+" );
//            long numNodes = Long.valueOf( parts[1] );
//            long chunkBase = Long.valueOf( parts[2].split( "-" )[0] );
//            String[] sampleNums = parts[3].substring( 1, parts[3].length() - 2 ).split( "-" );
//            List<Long> samples = new ArrayList<Long>();
//            for ( String sampleNum : sampleNums )
//            {
//                samples.add( Long.valueOf( sampleNum ) );
//            }
//            chunks.put( getChunkKeyForCount( chunkBase ), new Chunk( numNodes, samples ) );
//        }
//
//    }

}
