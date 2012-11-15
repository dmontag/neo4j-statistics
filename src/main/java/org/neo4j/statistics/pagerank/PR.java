package org.neo4j.statistics.pagerank;

import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.AbstractGraphDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

class PR {
    public static final int AVAILABLE_PROCS = Runtime.getRuntime().availableProcessors();
    private GraphDatabaseService graphDb;
    private ExecutorService executorService;
    private List<Walker> walkers = new ArrayList<Walker>();
    private Random random = new Random(1337);
    private double[] values;
    private int size;

    public PR(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        this.executorService = new ScheduledThreadPoolExecutor(AVAILABLE_PROCS);
        long longSize = ((AbstractGraphDatabase)graphDb).getNodeManager().getHighestPossibleIdInUse(Node.class);
        if (longSize >= Integer.MAX_VALUE) throw new IllegalArgumentException("Too many nodes.");
        size = (int) longSize;
        values = new double[size];
        Arrays.fill(values, 1.0/size);
        System.out.println(String.format("PR size=%d", size));
    }

    public void start() {
        for (int i = 0; i < AVAILABLE_PROCS; i++) {
            addWalker();
        }
    }

    private void addWalker() {
        int startPos = random.nextInt(size);
        Walker walker = new Walker(startPos);
        walkers.add(walker);
        executorService.submit(walker);
        System.out.println(String.format("Added walker: %d", startPos));
    }

    public double get(int id) {
        if (id >= size) throw new IllegalStateException("No such ID: " + id);
        return values[id];
    }

    private double getValueForNode(Node node) {
        return get((int) node.getId());
    }

    private void setValueForNode(Node node, double value) {
        int id = (int) node.getId();
        if (id >= size) throw new IllegalStateException("No such ID: " + id);
        values[id] = value;
    }

    public void stop() {
        for (Walker walker : walkers) {
            walker.stop();
        }
        executorService.shutdown();
    }

    public double[] values() {
        return values;
    }

    class Walker implements Runnable {
        private boolean stop;

        private int pos;

        public Walker(int startPos) {
            pos = startPos;
        }

        @Override
        public void run() {
            try {
                innerRun();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void innerRun() {
            while (!stop) {
                try {
                    Node node = graphDb.getNodeById(pos);

                    double rank = 1/size;
                    for (Relationship rel : node.getRelationships(Direction.INCOMING)) {
                        Node otherNode = rel.getOtherNode(node);
                        int otherOutgoing = IteratorUtil.count(otherNode.getRelationships(Direction.OUTGOING));
//                        System.out.println(String.format("other outgoing from %d: %d", node.getId(), otherOutgoing));
                        double v = getValueForNode(otherNode) / otherOutgoing;
                        rank += v;
//                        System.out.println(String.format("rank for %d: %f %f", node.getId(), rank, v));
                    }

                    setValueForNode(node, rank);
//                    System.out.println(String.format("%d <- %f", node.getId(), rank));
                } catch (NotFoundException e) {
                    System.out.println(String.format("Node %d not found, skipping.", pos));
                } catch (IllegalStateException e) {
//                    System.out.println(e.getMessage());
                }
                pos = random.nextInt(size);
            }
        }

        void stop() {
            stop = true;
        }

    }

}
