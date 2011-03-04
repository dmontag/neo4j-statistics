java -server -d64 -Xmx1000m -Xms1000m -XX:+UseConcMarkSweepGC -verbose:gc -cp target/neo4j-statistics-0.1-SNAPSHOT.jar:target/dependency/\* org.neo4j.statistics.RelationshipTypeCounts $1
