package org.neo4j.statistics;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

class Neo4jRunner extends Runner
{
    BlockJUnit4ClassRunner runner;
    private GraphDatabaseService graphDb;
    private File dbPath;
    private boolean clean;

    public Neo4jRunner( final Class<?> clazz ) throws InitializationError
    {
        runner = new BlockJUnit4ClassRunner( clazz )
        {
            @Override
            protected Statement withBeforeClasses( final Statement statement )
            {
                System.out.println("withBeforeClasses");
                return new Statement()
                {
                    @Override
                    public void evaluate() throws Throwable
                    {
                        applyInitAnnotations( clazz );
                        statement.evaluate();
                    }
                };
            }

            @Override
            protected Statement withAfterClasses( final Statement statement )
            {
                System.out.println("withAfterClasses");
                return new Statement()
                {
                    @Override
                    public void evaluate() throws Throwable
                    {
                        applyShutdownAnnotations( clazz );
                        statement.evaluate();
                    }
                };
            }

            //
//            @Override
//            protected List<MethodRule> rules( Object test )
//            {
//                List<MethodRule> result = new LinkedList<MethodRule>();
//                result.add( new MethodRule()
//                {
//                    public Statement apply( Statement base, FrameworkMethod method, Object target )
//                    {
//
//                        return base;
//                    }
//                } );
//                result.addAll( super.rules( test ) );
//                return result;
//            }


        };
    }

    private void applyShutdownAnnotations( Class<?> clazz )
    {
        graphDb.shutdown();
        if ( clean )
        {
            deleteFileOrDirectory( dbPath );
        }
    }

    private void applyInitAnnotations( Class<?> clazz )
    {
        for ( Field field : clazz.getDeclaredFields() )
        {
            GraphDb annotation = field.getAnnotation( GraphDb.class );
            if ( annotation != null )
            {
                clean = annotation.clean();
                initGraphDb( clazz, field );
                return;
            }
        }
    }

    private void initGraphDb( Object target, Field field )
    {
        if ( graphDb != null )
        {
            throw new RuntimeException( "Can only have one graph db currently." );
        }
        dbPath = getTempPath();
        graphDb = new EmbeddedGraphDatabase( dbPath.getAbsolutePath() );

        field.setAccessible( true );
        try
        {
            field.set( target, graphDb );
        }
        catch ( IllegalAccessException e )
        {
            throw new RuntimeException( "Unable to set field: " + field, e );
        }
    }

    private File getTempPath()
    {
        try
        {
            File tempFile = File.createTempFile( "neo-", "" );
            tempFile.delete();
            return tempFile;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Could not create tempfile.", e );
        }
    }

    @Override
    public Description getDescription()
    {
        return runner.getDescription();
    }

    @Override
    public void run( RunNotifier notifier )
    {
        runner.run( notifier );
    }


    public static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        file.delete();
    }
}
