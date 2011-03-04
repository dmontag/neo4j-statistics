package org.neo4j.statistics;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RelationshipsPerNodeHistogramTest extends Neo4jTestCase
{
    private static final DynamicRelationshipType REL_TYPE = DynamicRelationshipType.withName( "TEST" );
    private Map<Long, Chunk> expected;

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
        expected = new HashMap<Long, Chunk>();
    }

    @Test
    public void testJustReferenceNode()
    {
        Map<Long, Chunk> result = getNodeHisto( 10 ).getCounts();
        assertEquals( "Wrong result.", Collections.singletonMap( 0L, new Chunk( 1 ) ), result );
    }

    @Test
    public void testTwoNodesWithRel()
    {
        graphDb().getReferenceNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 10 ).getCounts();
        assertEquals( "Wrong result.", Collections.singletonMap( 1L, new Chunk( 2 ) ), result );
    }

    @Test
    public void testTwoNodesWithRelAndLonelyNode()
    {
        graphDb().createNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 10 ).getCounts();
        expected.put( 0L, new Chunk( 1 ) );
        expected.put( 1L, new Chunk( 2 ) );
        assertEquals( "Wrong result.", expected, result );
    }

    @Test
    public void testWrapsToNextChunk()
    {
        Node node = graphDb().createNode();
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 1 ).getCounts();
        expected.put( 0L, new Chunk( 1 ) );
        expected.put( 1L, new Chunk( 2 ) );
        expected.put( 2L, new Chunk( 1 ) );
        assertEquals( "Wrong result.", expected, result );
    }

    @Test
    public void testWrapsAcrossChunks()
    {
        Node node = graphDb().createNode();
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 1 ).getCounts();
        expected.put( 0L, new Chunk( 1 ) );
        expected.put( 1L, new Chunk( 3 ) );
        expected.put( 3L, new Chunk( 1 ) );
        assertEquals( "Wrong result.", expected, result );
    }

    @Test
    public void testWrapsToNextChunkAndMods()
    {
        Node node = graphDb().createNode();
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 2 ).getCounts();
        expected.put( 0L, new Chunk( 1 ) );
        expected.put( 1L, new Chunk( 3 ) );
        assertEquals( "Wrong result.", expected, result );
    }

    @Test
    public void testWrapsToNextChunkAndModsMultiple()
    {
        Node node = graphDb().createNode();
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 2 ).getCounts();
        expected.put( 0L, new Chunk( 1 ) );
        expected.put( 1L, new Chunk( 3 ) );
        expected.put( 2L, new Chunk( 1 ) );
        assertEquals( "Wrong result.", expected, result );
    }

    @Test
    public void testSamples()
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        Node node3 = graphDb().createNode();
        node1.createRelationshipTo( node2, REL_TYPE );
        node1.createRelationshipTo( node3, REL_TYPE );
        node1.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        node1.createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk> result = getNodeHisto( 10 ).getCounts();
        assertEquals( "Wrong samples.",
            Arrays.asList( node1.getId(), node2.getId(), node3.getId() ),
            result.get( 1L ).getSamples() );
    }

    @Test
    public void testWriteRow()
    {
        StringBuilder stringBuilder = new StringBuilder();
        Chunk chunk = new Chunk( 2L );
        chunk.record( 123L );
        new RelationshipsPerNodeHistogram( 5 ).writeRow( stringBuilder, 5, 2, chunk );
        assertEquals( "Wrong row logged.",
            Arrays.asList( "5", "3", "6-10", "[123]" ),
            Arrays.asList( splitRow( stringBuilder ) ) );
    }

    @Test
    public void testWriteRowZeroRels()
    {
        StringBuilder stringBuilder = new StringBuilder();
        Chunk chunk = new Chunk( 2L );
        chunk.record( 123L );
        chunk.record( 456L );
        new RelationshipsPerNodeHistogram( 5 ).writeRow( stringBuilder, 5, 0, chunk );
        assertEquals( "Wrong row logged.",
            Arrays.asList( "5", "4", "0", "[123,456]" ),
            Arrays.asList( splitRow( stringBuilder ) ) );
    }

    private String[] splitRow( StringBuilder stringBuilder )
    {
        return stringBuilder.toString().trim().replaceAll( "\t+", "\t" ).replaceAll( " ", "" ).split( "\t" );
    }

    @Test
    public void testNodeCount()
    {
        graphDb().getReferenceNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        assertEquals( "Wrong node count.", 2, getNodeHisto( 10 ).getNodeCount() );
    }

    @Test
    public void testRelCount()
    {
        graphDb().getReferenceNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        assertEquals( "Wrong rel count.", 1, getNodeHisto( 10 ).getRelCount() );
    }

    private RelationshipsPerNodeHistogram getNodeHisto( int chunkSize )
    {
        RelationshipsPerNodeHistogram histogram = new RelationshipsPerNodeHistogram( chunkSize );
        Map<Long, Chunk> counts = histogram.run( graphDb(), System.out ).getCounts();
        p( histogram );
        return histogram;
    }

    private void p( Object o )
    {
        System.out.println( o );
    }

}
