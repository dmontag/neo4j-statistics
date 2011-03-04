package org.neo4j.statistics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RelationshipsPerNodeHistogram
{
    private KeyedCounter<String> countPerType = new KeyedCounter<String>();
    private Map<Long, Chunk> chunks = new HashMap<Long, Chunk>();
    private long chunkSize;
    private long nodeCount;
    private long relCount;

    public RelationshipsPerNodeHistogram()
    {
        this( 5 );
    }

    public RelationshipsPerNodeHistogram( long chunkSize )
    {
        this.chunkSize = chunkSize;
    }

    public RelationshipsPerNodeHistogram run( GraphDatabaseService graphDb, PrintStream out )
    {
        for ( Node node : graphDb.getAllNodes() )
        {
            if ( ++nodeCount % 1000 == 0 ) out.print( nodeCount + "." );
            if ( nodeCount % 100000 == 0 ) out.print( this );
            int count = 0;
            for ( Relationship rel : node.getRelationships() )
            {
                countPerType.incForKey( rel.getType().name() );
                relCount++;
                count++;
            }
            long key = getChunkKeyForCount( count );
            Chunk chunk = chunks.get( key );
            if ( chunk == null ) chunks.put( key, chunk = new Chunk() );
            chunk.record( node.getId() );
        }
        return this;
    }

    private long getChunkKeyForCount( long count )
    {
        return count == 0 ? 0 : ( ( count - 1 ) / chunkSize ) + 1;
    }

    private long getCountBaseForChunkKey( long key )
    {
        return ( ( key - 1 ) * chunkSize ) + 1;
    }

    private long getEndOfChunkForCountBase( long countBase )
    {
        return countBase + chunkSize - 1;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();

        int rank = 1;
        result.append( "Total nodes: " ).append( getNodeCount() ).append( "\n" );
        result.append( "Total rels: " ).append( getRelCount() ).append( "\n" );
        result.append( "Rank\tNodes\t\tRels\t\tSamples" ).append( "\n" );
        TreeMap<Long, Chunk> sortedResults = new TreeMap<Long, Chunk>( new ReverseLongComparator() );
        sortedResults.putAll( chunks );
        for ( Map.Entry<Long, Chunk> relChunkEntry : sortedResults.entrySet() )
        {
            writeRow( result, rank++, relChunkEntry.getKey(), relChunkEntry.getValue() );
        }

        return result.toString();
    }

    public void writeRow( StringBuilder result, int rank, long chunkKey, Chunk chunk )
    {
        result.append( rank ).append( "\t" )
            .append( chunk.getCount() ).append( "\t\t" )
            .append( getRangeDescription( chunkKey ) ).append( "\t\t" )
            .append( chunk.getSamples() ).append( "\n" );
    }

    private String getRangeDescription( long chunkKey )
    {
        if ( chunkKey == 0 )
        {
            return "0";
        }
        long floor = getCountBaseForChunkKey( chunkKey );
        return floor + "-" + getEndOfChunkForCountBase( floor );
    }

    public Map<Long, Chunk> getCounts()
    {
        return chunks;
    }

    public long getNodeCount()
    {
        return nodeCount;
    }

    public long getRelCount()
    {
        return relCount / 2;
    }

    private static class ReverseLongComparator implements Comparator<Long>
    {

        public int compare( Long o1, Long o2 )
        {
            return ( o2 < o1 ) ? -1 : ( ( o1 < o2 ) ? 1 : 0 );
        }
    }

    public static void main( String[] args ) throws IOException
    {
        String path = args[0];
        long chunkSize = Long.valueOf( args[1] );
        String initDataFile = null;
        if (args.length > 2)
        {
            initDataFile = args[2];
        }
        File configFile = new File( new File( path ), "neo4j.properties" );
        Map<String, String> config = configFile.exists()
            ? EmbeddedGraphDatabase.loadConfigurations( configFile.getAbsolutePath() )
            : new HashMap<String, String>();
        GraphDatabaseService graphDb = new EmbeddedReadOnlyGraphDatabase( path, config );
        RelationshipsPerNodeHistogram result;
        try
        {
            RelationshipsPerNodeHistogram histogram = new RelationshipsPerNodeHistogram( chunkSize );
            if ( initDataFile != null )
            {
                histogram.init( new File( initDataFile ) );
            }
            result = histogram.run( graphDb, System.out );
        }
        finally
        {
            graphDb.shutdown();
        }
        System.out.println( result );
    }

    private void init( File file ) throws IOException
    {
        BufferedReader reader = new BufferedReader( new FileReader( file ) );
        for ( String line = reader.readLine(); line != null; line = reader.readLine() )
        {
            String[] parts = line.trim().split( "\\s+" );
            long numNodes = Long.valueOf( parts[1] );
            long chunkBase = Long.valueOf( parts[2].split( "-" )[0] );
            String[] sampleNums = parts[3].substring( 1, parts[3].length()-2 ).split( "-" );
            List<Long> samples = new ArrayList<Long>();
            for ( String sampleNum : sampleNums )
            {
                samples.add( Long.valueOf( sampleNum ) );
            }
            chunks.put( getChunkKeyForCount( chunkBase ), new Chunk( numNodes, samples ) );
        }

    }
}
