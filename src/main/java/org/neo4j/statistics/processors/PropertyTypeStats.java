package org.neo4j.statistics.processors;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.statistics.Counter;
import org.neo4j.statistics.KeyedCounter;
import org.neo4j.statistics.StatisticsProcessor;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.Map;

public class PropertyTypeStats implements StatisticsProcessor
{
    private GraphDatabaseService graphDb;
    private PrintStream out;
    private PropertyKeyedCounter propertyTypeOccurrences = new PropertyKeyedCounter();
    private long propertyCount;
    private volatile boolean shouldAbort;

    public PropertyTypeStats( GraphDatabaseService graphDb, PrintStream out )
    {
        this.graphDb = graphDb;
        this.out = out;
    }

    @Override
    public void process()
    {
        for ( Node node : graphDb.getAllNodes() )
        {
            if ( shouldAbort ) return;
            countProperties( node );
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                countProperties( rel );
            }
        }
    }

    private void countProperties( PropertyContainer node )
    {
        for ( String key : node.getPropertyKeys() )
        {
            propertyCount++;
            Object value = node.getProperty( key );
            propertyTypeOccurrences.incForProperty( value );
        }
    }

    @Override
    public void reportProgress()
    {
        out.println( String.format( "Total number of properties: %d", propertyCount ) );
        out.print( propertyTypeOccurrences );
    }

    @Override
    public void abort()
    {
        shouldAbort = true;
    }

    private static class PropertyKeyedCounter
    {
        KeyedCounter<Class> typeOccurrences = new KeyedCounter<Class>();
        KeyedCounter<Class> maxArraySize = new KeyedCounter<Class>();
        KeyedCounter<Class> summedArraySize = new KeyedCounter<Class>();

        public void incForProperty( Object value )
        {
            Class<? extends Object> type = value.getClass();
            typeOccurrences.incForKey( type );
            if ( isLengthable( type ) )
            {
                int length;
                if ( type == String.class )
                {
                    length = ((String) value).getBytes().length;
                }
                else
                {
                    length = Array.getLength( value );
                }
                summedArraySize.incForKey( type, length );
                if ( length > maxArraySize.getForKey( type ) )
                {
                    maxArraySize.setForKey( type, length );
                }
            }
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "Type\t\tCount\n" );
            for ( Map.Entry<Class, Counter> propertyTypeEntry : typeOccurrences.getCounters().entrySet() )
            {
                Class type = propertyTypeEntry.getKey();
                int occurrences = propertyTypeEntry.getValue().getCount();
                sb.append( String.format( "%s\t\t%d%s\n",
                    type.getSimpleName(),
                    occurrences,
                    ( isLengthable( type ) ? String.format( " (maxlen %dB, avg %dB)", maxArraySize.getForKey( type ), summedArraySize.getForKey( type ) / occurrences ) : "" ) ) );
            }
            return sb.toString();
        }

        private boolean isLengthable( Class type )
        {
            return type.isArray() || type == String.class;
        }
    }
}
