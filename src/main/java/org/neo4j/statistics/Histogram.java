package org.neo4j.statistics;

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Histogram<SAMPLE_TYPE>
{
    private Map<Long, Chunk<SAMPLE_TYPE>> chunks = new HashMap<Long, Chunk<SAMPLE_TYPE>>();
    private long chunkSize;
    private long totalSamples;
    private long totalCounts;
    private boolean includeSamples;

    public Histogram( long chunkSize )
    {
       this( chunkSize, true );
    }

    public Histogram( long chunkSize, boolean includeSamples )
    {
        this.chunkSize = chunkSize;
        this.includeSamples = includeSamples;
    }

    public void record( SAMPLE_TYPE sample, int countForSample )
    {
        totalSamples++;
        totalCounts += countForSample;
        long key = getChunkKeyForCount( countForSample );
        Chunk chunk = chunks.get( key );
        if ( chunk == null ) chunks.put( key, chunk = new Chunk() );
        chunk.record( sample );
    }

    private long getChunkKeyForCount( long count )
    {
        return count == 0 ? 0 : ( ( count - 1 ) / chunkSize ) + 1;
    }

    public long getCountBaseForChunkKey( long key )
    {
        return ( ( key - 1 ) * chunkSize ) + 1;
    }

    public long getEndOfChunkForCountBase( long countBase )
    {
        return countBase + chunkSize - 1;
    }

    public Map<Long, Chunk<SAMPLE_TYPE>> getChunks()
    {
        return chunks;
    }

    public long getTotalSamples()
    {
        return totalSamples;
    }

    public long getTotalCounts()
    {
        return totalCounts;
    }

    public void writeRows( StringBuilder result, SortedMap<Long, Chunk<SAMPLE_TYPE>> sortedResults )
    {
        int rank = 1;
        long aggregateCount = 0;
        long aggregateWeight = 0;
        for ( Map.Entry<Long, Chunk<SAMPLE_TYPE>> relChunkEntry : sortedResults.entrySet() )
        {
            long chunkKey = relChunkEntry.getKey();
            Chunk<SAMPLE_TYPE> chunk = relChunkEntry.getValue();
            long count = chunk.getCount();
            aggregateCount += count;
            long topOfChunk = chunkKey == 0 ? 0 : getEndOfChunkForCountBase( getCountBaseForChunkKey( chunkKey ) );
            long weight = count * topOfChunk;
            aggregateWeight += weight;
            writeRow( result, rank, count, chunkKey, chunk.getSamples(), aggregateCount, weight, aggregateWeight );
            rank += 1;
        }
    }

    @SuppressWarnings( { "unchecked" } )
    public void writeRow( StringBuilder result, int rank, long count, long chunkKey, List<SAMPLE_TYPE> samples, long aggregate, long weight, long aggregateWeight )
    {
        result.append( rank ).append( "\t" )
            .append( count ).append( "\t\t" )
            .append( getRangeDescription( chunkKey ) ).append( "\t\t" );
        if (includeSamples)
        {
            appendSamples( result, samples );
        }
        else
        {
            result.append( "[snip]\t\t" );
        }
        result.append( aggregate ).append( "\t\t" )
            .append( weight ).append( "\t\t" )
            .append( aggregateWeight ).append( "\n" );
    }

    private void appendSamples( StringBuilder result, List<SAMPLE_TYPE> samples )
    {
        result.append( "[" );
        Iterator<SAMPLE_TYPE> sampleIterator = samples.iterator();
        while ( sampleIterator.hasNext() )
        {
            SAMPLE_TYPE next = sampleIterator.next();
            if (next.getClass().isArray())
            {
                result.append( "[" );
                int arrayLen = Array.getLength( next );
                for ( int i = 0; i < arrayLen; i++ )
                {
                    result.append( Array.get( next, i ) );
                    if (i < arrayLen - 1) result.append( ", " );
                }
                result.append( "]" );
            }
            else
            {
                result.append( next );
            }
            if (sampleIterator.hasNext()) result.append( ", " );
        }
        result.append( "]" ).append( "\t\t" );
    }


    public String getRangeDescription( long chunkKey )
    {
        if ( chunkKey == 0 )
        {
            return "0";
        }
        long floor = getCountBaseForChunkKey( chunkKey );
        return floor + "-" + getEndOfChunkForCountBase( floor );
    }

    public String toString( String sampleType, String countType )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "Total " ).append( sampleType.toLowerCase() ).append( ": " ).append( totalSamples ).append( "\n" );
        sb.append( "Total " ).append( countType.toLowerCase() ).append( ": " ).append( totalCounts ).append( "\n" );
        sb.append( "Rank\t" ).append( sampleType ).append( "\t\t" ).append( countType )
            .append( "\t\tSamples\t\tAggregate from top\t\tWeight\t\tAggregate weight" ).append( "\n" );
        writeRows( sb, getSortedChunks() );
        return sb.toString();
    }

    private TreeMap<Long, Chunk<SAMPLE_TYPE>> getSortedChunks()
    {
        TreeMap<Long, Chunk<SAMPLE_TYPE>> sortedResults = new TreeMap<Long, Chunk<SAMPLE_TYPE>>( new ReverseLongComparator() );
        sortedResults.putAll( chunks );
        return sortedResults;
    }

    public static class ReverseLongComparator implements Comparator<Long>
    {
        public int compare( Long o1, Long o2 )
        {
            return ( o2 < o1 ) ? -1 : ( ( o1 < o2 ) ? 1 : 0 );
        }
    }
}
