package org.neo4j.statistics;

import java.util.HashMap;
import java.util.Map;

public class KeyedCounter<KEY>
{
    public Map<KEY, Counter> counters;

    public KeyedCounter()
    {
        this(new HashMap<KEY, Counter>() );
    }

    public KeyedCounter( Map<KEY, Counter> counters )
    {
        this.counters = counters;
    }

    public void incForKey( KEY key )
    {
        getCounter( key ).inc();
    }

    public void incForKey( KEY key, int increment )
    {
        getCounter( key ).add( increment );
    }

    public void setForKey( KEY key, int value )
    {
        getCounter( key ).set( value );
    }

    public int getForKey( KEY key )
    {
        return getCounter( key ).getCount();
    }

    private Counter getCounter( KEY key )
    {
        Counter counter = counters.get( key );
        if ( counter == null ) counters.put( key, counter = new Counter() );
        return counter;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        KeyedCounter that = (KeyedCounter) o;

        if ( counters != null ? !counters.equals( that.counters ) : that.counters != null ) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return counters != null ? counters.hashCode() : 0;
    }

    public Map<KEY, Counter> getCounters()
    {
        return counters;
    }
}
