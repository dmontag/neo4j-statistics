package org.neo4j.statistics.processors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.statistics.Counter;
import org.neo4j.statistics.KeyedCounter;
import org.neo4j.statistics.StatisticsProcessor;

import java.io.PrintStream;
import java.util.Map;

public class RelationshipTypeCounts implements StatisticsProcessor
{
    private KeyedCounter<String> countPerType = new KeyedCounter<String>();
    private long relCount;
	private PrintStream out;
    private GraphDatabaseService graphDb;
    private volatile boolean shouldAbort;

    public RelationshipTypeCounts( GraphDatabaseService graphDb, PrintStream out )
    {
        this.graphDb = graphDb;
        this.out = out;
    }

    public void run()
    {
        long maxRels = ( (AbstractGraphDatabase) graphDb ).getConfig().getGraphDbModule().getNodeManager().getNumberOfIdsInUse( Relationship.class ) + 1;
        out.println( "Max rels: " + maxRels );
        for ( int i = 0; i < maxRels; i++ )
        {
            if (shouldAbort) return;
            try
            {
                Relationship rel = graphDb.getRelationshipById( i );
                countPerType.incForKey( rel.getType().name() );
	            relCount++;
            }
            catch ( NotFoundException e )
            {
            }
        }
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();

	    result.append("Total: ").append(relCount).append("\n");
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

//    public static void main( String[] args )
//    {
//        String path = args[0];
//        File configFile = new File( new File( path ), "neo4j.properties" );
//        Map<String, String> config = configFile.exists()
//            ? EmbeddedGraphDatabase.loadConfigurations( configFile.getAbsolutePath() )
//            : new HashMap<String, String>();
//        GraphDatabaseService graphDb = new EmbeddedReadOnlyGraphDatabase( path, config );
//        RelationshipTypeCounts result;
//        try
//        {
//            result = new RelationshipTypeCounts().run( graphDb, System.out );
//        }
//        finally
//        {
//            graphDb.shutdown();
//        }
//        System.out.println( result );
//    }

	public void process() {
		run();
	}

	public void reportProgress() {
		out.print(this);
	}

    public void abort()
    {
        shouldAbort = true;
    }
}