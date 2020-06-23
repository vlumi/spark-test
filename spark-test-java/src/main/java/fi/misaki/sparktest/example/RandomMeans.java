package fi.misaki.sparktest.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.List;

/**
 * Calculate the mean of a random variables.
 * <p>
 * The purpose of this class is to show how persisting the intermediate data
 * will change the behavior of the processing.
 */
public class RandomMeans implements Serializable {
    private transient JavaSparkContext sc;

    public RandomMeans(JavaSparkContext sc) {
        this.sc = sc;
    }


    public double summarizeWithoutPersist(List<Integer> data) {
        return summarize(data, false);
    }

    public double summarizeWithPersist(List<Integer> data) {
        return summarize(data, true);
    }

    public double summarize(List<Integer> data, boolean persist) {
        LongAccumulator counter = sc.sc().longAccumulator();
        JavaRDD<Double> randomData = sc.parallelize(data)
                .map(value -> {
                    counter.add(1);
                    return Math.random();
                });
        if (persist) {
            randomData.persist(StorageLevel.MEMORY_ONLY());
        }

        Double mean1 = randomData.reduce(this::mean);
        Double mean2 = randomData.reduce(this::mean);

        long count = counter.sum();
        System.out.println("Number of input values counted: " + count);

        return Math.abs(mean1 - mean2);
    }

    private  double mean(double a, double b) {
        return (a + b) / 2;
    }

}
