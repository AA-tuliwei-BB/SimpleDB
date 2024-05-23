package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] histogram;
    private int min;
    private int max;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        if (buckets > max - min + 1) {
            buckets = max - min + 1;
        }
        this.histogram = new int[buckets];
        this.min = min;
        this.max = max;
        this.ntups = 0;
        for (int i = 0; i < buckets; i++) {
            this.histogram[i] = 0;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int bucket = (v - min) * histogram.length / (max - min + 1);
        histogram[bucket]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int bucket = (v - min) * histogram.length / (max - min + 1);
        int left = bucket * (max - min + 1) / histogram.length + min;
        int right = (bucket + 1) * (max - min + 1) / histogram.length + min;
        double selectivity = 0.0;
        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    selectivity = 0.0;
                } else {
                    selectivity = (double) histogram[bucket] / (right - left) / ntups;
                }
                break;
            case NOT_EQUALS:
                if (v < min || v > max) {
                    selectivity = 1.0;
                } else {
                    selectivity = 1.0 - (double) histogram[bucket] / (right - left);
                }
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                if (v < min) {
                    selectivity = 1.0;
                } else if (v > max) {
                    selectivity = 0.0;
                } else {
                    int total = 0;
                    for (int i = bucket + 1; i < histogram.length; i++) {
                        total += histogram[i];
                    }
                    if (op == Predicate.Op.GREATER_THAN) {
                        selectivity = (double) histogram[bucket] * (right - v - 1) / (right - left);
                    } else {
                        selectivity = (double) histogram[bucket] * (right - v) / (right - left);
                    }
                    selectivity = (selectivity + total) / ntups;
                }
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                if (v < min) {
                    selectivity = 0.0;
                } else if (v > max) {
                    selectivity = 1.0;
                } else {
                    int total = 0;
                    for (int i = 0; i < bucket; i++) {
                        total += histogram[i];
                    }
                    if (op == Predicate.Op.LESS_THAN) {
                        selectivity = (double) histogram[bucket] * (v - left) / (right - left);
                    } else {
                        selectivity = (double) histogram[bucket] * (v - left + 1) / (right - left);
                    }
                    selectivity = (selectivity + total) / ntups;
                }
                break;
            case LIKE:
                selectivity = 1.0;
                break;
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        sb.append("Histogram: ");
        sb.append(histogram.length);
        sb.append(" ");
        sb.append(min);
        sb.append(" ");
        sb.append(max);
        sb.append(" ");
        sb.append(ntups);
        sb.append(" ");
        for (int i = 0; i < histogram.length; i++) {
            sb.append(histogram[i]);
            sb.append(" ");
        }
        return sb.toString();
    }
}
