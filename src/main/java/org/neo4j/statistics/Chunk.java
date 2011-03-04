package org.neo4j.statistics;

import java.util.ArrayList;
import java.util.List;

public class Chunk
{
    private List<Long> samples = new ArrayList<Long>();
    private long count;

    public Chunk()
    {
        this( 0L );
    }

    public Chunk( long initialCount )
    {
        this.count = initialCount;
    }

    public Chunk( long initialCount, List<Long> samples )
    {
        this.count = initialCount;
        this.samples = samples;
    }

    public void record( long sample )
    {
        count++;
        if ( samples.size() < 3 )
        {
            samples.add( sample );
        }
    }

    public List<Long> getSamples()
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
