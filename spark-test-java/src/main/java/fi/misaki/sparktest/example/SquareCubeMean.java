package fi.misaki.sparktest.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.List;

/**
 * Calculate the mean of a bunch of random values that have been squared and
 * cubed, separately, with the result union'd together.
 * <p>
 * In this example, the main data is forked into two branches, and joined
 * later together to get the final result.
 */
public class SquareCubeMean implements Serializable {
    private transient JavaSparkContext sc;

    public SquareCubeMean(JavaSparkContext sc) {
        this.sc = sc;
    }

    public double process(List<Integer> data) {
        JavaRDD<Double> randomData = sc.parallelize(data)
                .map(value -> Math.random());
        randomData.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<Double> squares = randomData
                .map(x -> x * x);
        JavaRDD<Double> cubes = randomData
                .map(x -> x * x * x);

        return squares.union(cubes)
                .reduce((a, b) -> (a + b) / 2);
    }

}
