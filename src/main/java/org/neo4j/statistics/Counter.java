package org.neo4j.statistics;

public class Counter
{
    private int count;

    public Counter()
    {
        this( 0 );
    }

    public Counter( int initialCount )
    {
        this.count = initialCount;
    }

    public int getCount()
    {
        return count;
    }

    public void inc()
    {
        count++;
    }

    public void add( int count )
    {
        this.count += count;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        Counter counter = (Counter) o;

        if ( count != counter.count ) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "" + count;
    }

    public void set( int value )
    {
        this.count = value;
    }
}
