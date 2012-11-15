package org.neo4j.statistics.pagerank;

import jline.ConsoleReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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

        try {
            int id = Integer.parseInt(cmd);
            println("%d: %s", id, Double.valueOf(pr.get(id)).toString());
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
