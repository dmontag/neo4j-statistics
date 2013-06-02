package org.neo4j.statistics;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.statistics.processors.RelationshipsPerNodeHistogram;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RelationshipsPerNodeHistogramTest extends Neo4jTestCase
{
    private static final DynamicRelationshipType REL_TYPE = DynamicRelationshipType.withName( "TEST" );
    private Map<Long, Chunk> expected;

    @Before
    public void cleanDb()
    {
        for ( Node node : GlobalGraphOperations.at(graphDb()).getAllNodes() )
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
        assertEquals("Clean failed.", 1, Iterables.count(GlobalGraphOperations.at(graphDb()).getAllNodes()));
        expected = new HashMap<Long, Chunk>();
    }

    @Test
    public void testJustReferenceNode()
    {
        Map<Long, Chunk<Long>> result = getNodeHisto( 10 ).getCounts();
        assertEquals( "Wrong result.", Collections.singletonMap( 0L, new Chunk( 1 ) ), result );
    }

    @Test
    public void testTwoNodesWithRel()
    {
        graphDb().getReferenceNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk<Long>> result = getNodeHisto( 10 ).getCounts();
        assertEquals( "Wrong result.", Collections.singletonMap( 1L, new Chunk( 2 ) ), result );
    }

    @Test
    public void testTwoNodesWithRelAndLonelyNode()
    {
        graphDb().createNode().createRelationshipTo( graphDb().createNode(), REL_TYPE );
        Map<Long, Chunk<Long>> result = getNodeHisto( 10 ).getCounts();
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
        Map<Long, Chunk<Long>> result = getNodeHisto( 1 ).getCounts();
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
        Map<Long, Chunk<Long>> result = getNodeHisto( 1 ).getCounts();
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
        Map<Long, Chunk<Long>> result = getNodeHisto( 2 ).getCounts();
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
        Map<Long, Chunk<Long>> result = getNodeHisto( 2 ).getCounts();
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
        Map<Long, Chunk<Long>> result = getNodeHisto( 10 ).getCounts();
        List<Long> samples = result.get(1L).getSamples();
        System.out.println(samples);
        assertTrue("Wrong samples.",
                samples.containsAll(Arrays.asList(node1.getId(), node2.getId(), node3.getId())));
    }

    @Test
    public void testWriteRows() {
        StringBuilder stringBuilder = new StringBuilder();
        SortedMap<Long,Chunk<Long>> results = new TreeMap<Long, Chunk<Long>>( new Histogram.ReverseLongComparator() );
        results.put( 2L, new Chunk<Long>( 2 ) ); // 6-10
        results.put( 1L, new Chunk<Long>( 3 ) ); // 1-5
        new RelationshipsPerNodeHistogram( graphDb(), System.out, 5 ).writeRows( stringBuilder, results );

        String[] lines = stringBuilder.toString().split( "\n" );
        assertEquals( "Wrong row logged.",
            Arrays.asList( "1", "2", "6-10", "[]", "2", "20", "20" ),
            splitRow( lines[0] ) );
        assertEquals( "Wrong row logged.",
            Arrays.asList( "2", "3", "1-5", "[]", "5", "15", "35" ),
            splitRow( lines[1] ) );
    }


    //writeRow( StringBuilder result, int rank, long chunkKey, Chunk<Long> chunk, long aggregate )
    @Test
    public void testWriteRow()
    {
        StringBuilder stringBuilder = new StringBuilder();
        Chunk<Long> chunk = new Chunk<Long>( 2L );
        chunk.record( 123L );
        new RelationshipsPerNodeHistogram( graphDb(), System.out, 5 ).writeRow( stringBuilder, 5, chunk.getCount(), 2, chunk.getSamples(), 30, 40, 50 );
        assertEquals( "Wrong row logged.",
            Arrays.asList( "5", "3", "6-10", "[123]", "30", "40", "50" ),
            splitRow( stringBuilder.toString() ) );
    }

    @Test
    public void testWriteRowZeroRels()
    {
        StringBuilder stringBuilder = new StringBuilder();
        Chunk<Long> chunk = new Chunk<Long>( 2L );
        chunk.record( 123L );
        chunk.record( 456L );
        new RelationshipsPerNodeHistogram( graphDb(), System.out, 5 ).writeRow( stringBuilder, 5, chunk.getCount(), 0, chunk.getSamples(), 3, 0, 0 );
        assertEquals( "Wrong row logged.",
            Arrays.asList( "5", "4", "0", "[123,456]", "3", "0", "0" ),
            splitRow( stringBuilder.toString() ) );
    }

    private List<String> splitRow( String str )
    {
        return Arrays.asList( str.trim().replaceAll( "\t+", "\t" ).replaceAll( " ", "" ).split( "\t" ) );
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
        RelationshipsPerNodeHistogram histogram = new RelationshipsPerNodeHistogram( graphDb(), System.out, chunkSize );
        histogram.run();
//        Map<Long, Chunk<Long>> counts = histogram.getCounts();
        p( histogram );
        return histogram;
    }

    private void p( Object o )
    {
        System.out.println( o );
    }

}
