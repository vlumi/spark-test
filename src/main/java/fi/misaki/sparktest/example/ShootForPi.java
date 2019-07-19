package fi.misaki.sparktest.example;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * This code estimates π by "throwing darts" at a circle.
 * We pick random points in the unit square ((0, 0) to (1,1))
 * and see how many fall in the unit circle.
 * The fraction should be π / 4, so we use this to get our estimate.
 * <p>
 * Original source: https://spark.apache.org/examples.html
 */
public class ShootForPi implements Serializable {
    private transient JavaSparkContext sc;

    public ShootForPi(JavaSparkContext sc) {
        this.sc = sc;
    }

    public double estimatePi(List<Integer> data) {
        long count = sc.parallelize(data)
                .filter(i -> {
                    double x = Math.random();
                    double y = Math.random();
                    return x * x + y * y < 1;
                })
                .count();
        return 4.0 * count / data.size();
    }
}
