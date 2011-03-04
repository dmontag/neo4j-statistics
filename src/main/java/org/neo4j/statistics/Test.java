package org.neo4j.statistics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.util.concurrent.CountDownLatch;

public class Test
{
    public static void main( String[] args ) throws InterruptedException
    {
        final GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "target/var/neo" );
        Transaction tx = graphDb.beginTx();
        try
        {
            graphDb.getReferenceNode().setProperty( "prop", "a" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        final CountDownLatch setPropLatch = new CountDownLatch( 1 );
        final CountDownLatch commitLatch = new CountDownLatch( 1 );
        final CountDownLatch committedLatch = new CountDownLatch( 1 );

        new Thread() {
            @Override
            public void run()
            {
                Transaction tx = graphDb.beginTx();
                try
                {
                    graphDb.getReferenceNode().setProperty( "prop", "b" );
                    setPropLatch.countDown();
                    commitLatch.await();
                    tx.success();
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
                finally
                {
                    tx.finish();
                }
                committedLatch.countDown();
            }
        }.start();

        setPropLatch.await();
        commitLatch.countDown();
        committedLatch.await();

        Object value = graphDb.getReferenceNode().getProperty( "prop" );
        System.out.println(value);


        graphDb.shutdown();
    }
}
