package org.neo4j.statistics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;

import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RelationshipTypeCounts
{
    private KeyedCounter<String> countPerType = new KeyedCounter<String>();
    private long relCount;

    public RelationshipTypeCounts run( GraphDatabaseService graphDb, PrintStream out )
    {
        long maxRels = ( (EmbeddedReadOnlyGraphDatabase) graphDb ).getConfig().getGraphDbModule().getNodeManager().getNumberOfIdsInUse( Relationship.class ) + 1;
        out.println( "Max rels: " + maxRels );
        for ( int i = 0; i < maxRels; i++ )
        {
            try
            {
                Relationship rel = graphDb.getRelationshipById( i );
                countPerType.incForKey( rel.getType().name() );
                if (++relCount % 1000 == 0) out.print(relCount + ".");
                if (relCount % 100000 == 0) out.print(this);
            }
            catch ( NotFoundException e )
            {
            }
        }
        return this;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();

        result.append( "Type\t\tCount" ).append( "\n" );
        for ( Map.Entry<String, Counter> entry : countPerType.getCounters().entrySet() )
        {
            writeRow( result, entry.getKey(), entry.getValue() );
        }

        return result.toString();
    }

    public void writeRow( StringBuilder result, String type, Counter counter )
    {
        result.append( type ).append( "\t\t" ).append( counter.getCount() ).append( "\n" );
    }

    public Map<String, Counter> getResult()
    {
        return countPerType.getCounters();
    }

    public static void main( String[] args )
    {
        String path = args[0];
        File configFile = new File( new File( path ), "neo4j.properties" );
        Map<String, String> config = configFile.exists()
            ? EmbeddedGraphDatabase.loadConfigurations( configFile.getAbsolutePath() )
            : new HashMap<String, String>();
        GraphDatabaseService graphDb = new EmbeddedReadOnlyGraphDatabase( path, config );
        RelationshipTypeCounts result;
        try
        {
            result = new RelationshipTypeCounts().run( graphDb, System.out );
        }
        finally
        {
            graphDb.shutdown();
        }
        System.out.println( result );
    }
}