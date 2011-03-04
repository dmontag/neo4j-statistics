package org.neo4j.statistics;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;

public class Neo4jJUnitRunner extends Runner
{
    Neo4jRunner runner;

    public Neo4jJUnitRunner( Class<?> clazz ) throws InitializationError
    {
        runner = new Neo4jRunner( clazz );
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

}
