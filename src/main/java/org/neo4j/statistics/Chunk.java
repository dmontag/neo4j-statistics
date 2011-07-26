package org.neo4j.statistics;

import java.util.ArrayList;
import java.util.List;

public class Chunk<SAMPLE_TYPE>
{
    private List<SAMPLE_TYPE> samples = new ArrayList<SAMPLE_TYPE>();
    private long count;

    public Chunk()
    {
        this( 0L );
    }

    public Chunk( long initialCount )
    {
        this.count = initialCount;
    }

    public Chunk( long initialCount, List<SAMPLE_TYPE> samples )
    {
        this.count = initialCount;
        this.samples = samples;
    }

    public void record( SAMPLE_TYPE sample )
    {
        count++;
        if ( samples.size() < 3 )
        {
            samples.add( sample );
        }
    }

    public List<SAMPLE_TYPE> getSamples()
    {
        return samples;
    }

    public long getCount()
    {
        return count;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        Chunk chunk = (Chunk) o;

        if ( count != chunk.count ) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) ( count ^ ( count >>> 32 ) );
    }
}
