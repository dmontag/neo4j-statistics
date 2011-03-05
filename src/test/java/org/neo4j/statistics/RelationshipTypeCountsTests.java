package org.neo4j.statistics;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.statistics.processors.RelationshipTypeCounts;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RelationshipTypeCountsTests extends Neo4jTestCase
{
    private static final DynamicRelationshipType REL_TYPE = DynamicRelationshipType.withName( "TEST" );
    private static final DynamicRelationshipType REL_TYPE2 = DynamicRelationshipType.withName( "TEST2" );
    private Map<String, Counter> expected;

    @Before
    public void cleanDb()
    {
        for ( Node node : graphDb().getAllNodes() )
        {
            if ( node.getId() == 0 )
            {
                continue;
            }
            for ( Relationship rel : node.getRelationships() )
            {
                rel.delete();
            }
            node.delete();
        }
        restartTx();
        expected = new HashMap<String, Counter>();
    }

    @Test
    public void testStatsOnEmptyGraph()
    {
        Map<String, Counter> result = getCounts().getResult();
        assertEquals( "Wrong result.", Collections.<String, Counter>emptyMap(), result );
    }

    @Test
    public void testStatsByRelType()
    {
        graphDb().getReferenceNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<String, Counter> result = getCounts().getResult();
        assertEquals( "Wrong result.", Collections.singletonMap( "TEST", new Counter( 1 ) ), result );
    }

    @Test
    public void testStatsByMultipleRelTypes()
    {
        Node node = graphDb().createNode();
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE2 );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE2 );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE2 );
        restartTx();
        Map<String, Counter> result = getCounts().getResult();
        expected.put( "TEST", new Counter( 1 ) );
        expected.put( "TEST2", new Counter( 3 ) );
        assertEquals( "Wrong result.", expected, result );
    }

    @Test
    public void testWriteRow()
    {
        StringBuilder stringBuilder = new StringBuilder();
        getCounts().writeRow( stringBuilder, "REL_TYPE", new Counter( 4 ) );
        assertEquals("",
            Arrays.asList("REL_TYPE", "4"),
            Arrays.asList(stringBuilder.toString().trim().replaceAll( "\t+", "\t" ).replaceAll( " ", "" ).split( "\t" ) ) );
    }

    private RelationshipTypeCounts getCounts()
    {
        RelationshipTypeCounts counts = new RelationshipTypeCounts( graphDb(), System.out );
        counts.run();
        p( counts );
        return counts;
    }

    private void p( Object o )
    {
        System.out.println( o );
    }

}
