package org.neo4j.statistics;

import java.util.Random;

public class CacheMissOnWrite
{
    private static final int TRIES = 3;
    private static final int ITERATIONS = 1 << 28;
    private static final int SIZE = 1 << 24;
    private static final int SPARSE_SHIFT = 7;
    private static final int MASK = SIZE - 1;
    private static final byte[] array = new byte[SIZE];

    public static void main( String[] args )
    {
        for ( int i = 0; i < SIZE; i++ )
        {
            array[i] = (byte) i;
        }

        System.out.println( String.format( "Using size: %d, mask: 0x%08X, sparse shift: %d", SIZE, MASK, SPARSE_SHIFT ) );

        System.out.println( String.format( "      sparse        single     sequential    random" ) );
        for ( int j = 1; j <= TRIES; j++ )
        {
            long sparse = runSparse();
            long single = runSingle();
            long sequential = runSequential();
            long random = runRandom();
            System.out.println( String.format( "#%d: %.3fms    %.3fms    %.3fms    %.3fms",
                j,
                sparse / 1000000.0,
                single / 1000000.0,
                sequential / 1000000.0,
                random / 1000000.0
            ) );
        }
        System.out.println( String.format( "result: %d", array[0] ) );
    }

    private static long runSparse()
    {
        long start = System.nanoTime();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            array[( i << SPARSE_SHIFT ) & MASK] = (byte) i;
        }
        return System.nanoTime() - start;
    }

    private static long runSingle()
    {
        long start = System.nanoTime();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            array[i & 1] = (byte) i;
        }
        return System.nanoTime() - start;
    }

    private static long runSequential()
    {
        long start = System.nanoTime();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            array[i & MASK] = (byte) i;
        }
        return System.nanoTime() - start;
    }

    private static long runRandom()
    {
        Random rand = new Random();
        long randomAdjustmentStart = System.nanoTime();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            rand.nextInt();
        }
        long randomAdjustment = System.nanoTime() - randomAdjustmentStart;
        long start = System.nanoTime();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            array[rand.nextInt() & MASK] = (byte) i;
        }
        return System.nanoTime() - start - randomAdjustment;
    }
}
