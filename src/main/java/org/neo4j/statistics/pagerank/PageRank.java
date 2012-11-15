package org.neo4j.statistics.pagerank;

import jline.ConsoleReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;

public class PageRank {

    private GraphDatabaseService graphDb;
    private File storePath;
    private ExecutorService executorService;
    private ConsoleReader reader;
    private PR pr;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            println("Missing arg: <store path>");
            System.exit(1);
        }
        PageRank main = new PageRank(args[0]);
        try {
            main.run();
        } finally {
            main.shutdown();
        }
    }

    public PageRank(String storePath) throws IOException {
        this.storePath = new File(storePath);
        reader = new ConsoleReader();
    }

    private void shutdown() {
        pr.stop();
        graphDb.shutdown();
    }

    private void run() throws Exception {
        graphDb = createGraphDb();
        startPageRank();
        while (handleCmd()) ;
    }

    private void startPageRank() {
        pr = new PR(graphDb);
        pr.start();
    }

    private GraphDatabaseService createGraphDb()
    {
        File configFile = new File( storePath, "neo4j.properties" );
        GraphDatabaseFactory f = new GraphDatabaseFactory();
        return configFile.exists()
                ? f.newEmbeddedDatabaseBuilder(storePath.getAbsolutePath()).loadPropertiesFromFile( configFile.getAbsolutePath() ).newGraphDatabase()
                : f.newEmbeddedDatabase(storePath.getAbsolutePath());
    }

    private boolean handleCmd() throws IOException {
        String cmd = reader.readLine("> ");
        if (cmd.isEmpty()) return true;
        if (cmd.startsWith("exit") || cmd.equalsIgnoreCase("quit")) return false;
        if (cmd.startsWith("values")) {
            println("%s", Arrays.toString(pr.values()));
            return true;
        }
        if (cmd.startsWith("sorted")) {
            double[] values = pr.values();
            Pair[] sorted = new Pair[values.length];
            for (int i = 0; i < values.length; i++) {
                sorted[i] = Pair.of(i, values[i]);
            }
            Arrays.sort(sorted, new Comparator<Pair<Integer, Double>>() {
                @Override
                public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                    return o1.other() < o2.other() ? -1 : 1;
                }
            });
            println("%s", Arrays.toString(sorted));
            return true;
        }

        try {
            int id = Integer.parseInt(cmd);
            println("%d: %f", id, pr.get(id));
        } catch (NumberFormatException e) {
            println("Invalid id: %s", cmd);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private static void println() {
        System.out.println();
    }

    private static void println(String format, Object... args) {
        System.out.println(String.format(format, args));
    }

}
