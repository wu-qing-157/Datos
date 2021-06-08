package simpledb;

import java.util.TreeMap;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int buckets;
    private final TreeMap<Integer, Integer> map = new TreeMap<>();
    private final int[] size, start;
    private final BIT count;

    private static class BIT {
        int n;
        int[] a;

        BIT(int n) {
            this.n = n;
            a = new int[n + 1];
        }

        private static int lowbit(int x) {
            return x & (-x);
        }

        void inc(int i) {
            for (i++; i <= n; i += lowbit(i)) a[i]++;
        }

        private int sum(int i) {
            int ans = 0;
            for (; i > 0; i -= lowbit(i)) ans += a[i];
            return ans;
        }

        int sum(int l, int r) {
            return sum(r + 1) - sum(l);
        }
    }

    private static int floorDiv(long a, int b) {
        if (a >= 0) return (int) (a / b);
        else return (int) ((a - b + 1) / b);
    }

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        max++;
        this.buckets = buckets;
        size = new int[buckets];
        start = new int[buckets];
        for (int i = 0; i < buckets; i++) {
            int c = floorDiv((long) min * (buckets - i) + (long) max * i, buckets);
            int next = floorDiv((long) min * (buckets - i - 1) + (long) max * (i + 1), buckets);
            map.put(c, i);
            start[i] = c;
            size[i] = next - c;
        }
        count = new BIT(buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        count.inc(map.floorEntry(v).getValue());
    }

    private double estimateCount(Predicate.Op op, int v) {
        int i = map.floorEntry(v).getValue();
        switch (op) {
            case EQUALS:
                return (double) count.sum(i, i) / size[i];
            case GREATER_THAN:
                return count.sum(i + 1, buckets - 1) + (double) count.sum(i, i) * (size[i] - v + start[i] - 1) / size[i];
            case LESS_THAN:
                return count.sum(0, i - 1) + (double) count.sum(i, i) * (v - start[i]) / size[i];
            case LESS_THAN_OR_EQ:
                return count.sum(0, i - 1) + (double) count.sum(i, i) * (v - start[i] + 1) / size[i];
            case GREATER_THAN_OR_EQ:
                return count.sum(i + 1, buckets - 1) + (double) count.sum(i, i) * (size[i] - v + start[i]) / size[i];
            case LIKE:
                throw new RuntimeException("What does LIKE mean to Int?");
            case NOT_EQUALS:
                return count.sum(0, buckets - 1) - (double) count.sum(i, i) / size[i];
            default:
                throw new RuntimeException("Why Java cannot determine I have switched over all enumerations?");
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v < start[0])
            return op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS ? 1 : 0;
        if (v >= start[buckets - 1] + size[buckets - 1])
            return op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS ? 1 : 0;
        return estimateCount(op, v) / count.sum(0, buckets - 1);
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        throw new RuntimeException("I do not think I need this");
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return start[0] + " " + (start[buckets - 1] + size[buckets - 1]);
    }
}
