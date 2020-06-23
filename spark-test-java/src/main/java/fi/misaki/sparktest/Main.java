package fi.misaki.sparktest;

import fi.misaki.sparktest.example.RandomMeans;
import fi.misaki.sparktest.example.ShootForPi;
import fi.misaki.sparktest.example.SquareCubeMean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;
import java.util.List;

/**
 * Simple examples for exploring Apache Spark.
 */
public class Main {
    private static final int WAIT_S = 60;
    private static final int WAIT_GRANULARITY_S = 10;
    private static final long MULTIPLIER_NS = 1_000_000_000L;
    private static final int MULTIPLIER_S = 1_000;

    public static void main(String... args) {
        JavaSparkContext sc = init();

        runShootForPi(sc);
        runRandomMeans(sc);
        runSquareCubeMean(sc);

        // Wait a while, to give some time for browsing the job data
        waitAtEnd(WAIT_S);
    }

    private static JavaSparkContext init() {
        SparkConf conf = new SparkConf()
                .setAppName("spark-test")
                .setMaster("local")
                .set("spark.driver.bindAddress", "127.0.0.1");
        return new JavaSparkContext(conf);
    }

    private static void runShootForPi(JavaSparkContext sc) {
        double pi = new ShootForPi(sc)
                .estimatePi(generateEmptyData(10_000_000));
        System.out.println("Pi is roughly " + pi);
    }

    private static void runRandomMeans(JavaSparkContext sc) {
        double meanDifference1 = new RandomMeans(sc)
                .summarizeWithoutPersist(generateEmptyData(10_000_000));
        System.out.println("Processed twice without persisting, means differ by " + meanDifference1);
        double meanDifference2 = new RandomMeans(sc)
                .summarizeWithPersist(generateEmptyData(10_000_000));
        System.out.println("Processed twice with persisting, means differ by " + meanDifference2);
    }

    private static void runSquareCubeMean(JavaSparkContext sc) {
        double mean = new SquareCubeMean(sc)
                .process(generateEmptyData(10_000_000));
        System.out.println("Mean from random values squared and cubed: " + mean);
    }

    private static List<Integer> generateEmptyData(int size) {
        return Collections.nCopies(size, 0);
    }

    private static void waitAtEnd(long seconds) {
        long endNanos = System.nanoTime() + seconds * MULTIPLIER_NS;
        try {
            while (System.nanoTime() < endNanos) {
                long secondsLeft = (endNanos - System.nanoTime()) / MULTIPLIER_NS;
                System.out.println("Time until termination: " + secondsLeft + "s");
                Thread.sleep(WAIT_GRANULARITY_S * MULTIPLIER_S);
            }
        } catch (InterruptedException e) {
        }
    }
}
