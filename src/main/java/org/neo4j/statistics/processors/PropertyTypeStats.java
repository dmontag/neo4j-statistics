package org.neo4j.statistics.processors;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.statistics.Counter;
import org.neo4j.statistics.Histogram;
import org.neo4j.statistics.KeyedCounter;
import org.neo4j.statistics.StatisticsProcessor;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class PropertyTypeStats implements StatisticsProcessor
{
    private GraphDatabaseService graphDb;
    private PrintStream out;
    private PropertyKeyedCounter propertyTypeOccurrences;
    private long propertyCount;
    private volatile boolean shouldAbort;

    public PropertyTypeStats( GraphDatabaseService graphDb, PrintStream out, long histoChunkSize )
    {
        this.graphDb = graphDb;
        this.out = out;
        propertyTypeOccurrences = new PropertyKeyedCounter( histoChunkSize );
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
        Map<String, Histogram<Object>> histograms = new HashMap<String, Histogram<Object>>();
        KeyedCounter<Class> typeOccurrences = new KeyedCounter<Class>();
        KeyedCounter<Class> maxArraySize = new KeyedCounter<Class>();
        KeyedCounter<Class> summedArraySize = new KeyedCounter<Class>();
        private long histoChunkSize;

        public PropertyKeyedCounter( long histoChunkSize )
        {
            this.histoChunkSize = histoChunkSize;
        }

        public void incForProperty( Object value )
        {
            Class<?> type = value.getClass();
            typeOccurrences.incForKey( type );
            if ( isLengthable( type ) )
            {
                int length;
                if ( type == String.class )
                {
                    length = ( (String) value ).getBytes( Charset.forName( "UTF-8" ) ).length;
                }
                else if ( type == int[].class )
                {
                    length = Array.getLength( value ) * 4;
                }
                else if (type == long[].class)
                {
                    length = Array.getLength( value ) * 8;
                }
                else if (type == short[].class)
                {
                    length = Array.getLength( value ) * 2;
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
                addToHisto( type, value, length );
            }
        }

        private void addToHisto( Class<?> type, Object value, int length )
        {
            String typeName = type.getSimpleName();
            Histogram<Object> histo = histograms.get( typeName );
            if (histo == null) histograms.put( typeName, histo = new Histogram<Object>( histoChunkSize, false ) );
            histo.record( value, length );
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
            sb.append( "\n" );
            for ( Map.Entry<String, Histogram<Object>> histogramEntry : histograms.entrySet() )
            {
                sb.append( "Histogram for " + histogramEntry.getKey() ).append( "\n" );
                sb.append( histogramEntry.getValue().toString( "Objects", "Bytes" ) );
                sb.append( "\n" );
            }
            return sb.toString();
        }

        private boolean isLengthable( Class type )
        {
            return type.isArray() || type == String.class;
        }
    }
}
