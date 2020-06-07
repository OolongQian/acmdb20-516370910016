package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private int buckets;
	private int min;
	private int max;
	private int totalTuple;
	
	private int[] bucketValues;
	private double bucketWidth;
	
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
		// some code goes here
		this.buckets = buckets;
		this.min = min;
		this.max = max;
		
		bucketValues = new int[this.buckets + 1];
		bucketWidth = (double) (this.max - this.min) / (buckets - 1);
		
		totalTuple = 0;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 *
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		int bucketIndex = valueToBucket(v);
		bucketValues[bucketIndex] += 1;
		totalTuple += 1;
	}
	
	/**
	 * Bucket interval bound, left closed right open. */
	private double leftBound(int i) {
		assert i >= 0 && i < buckets;
		return min + i * bucketWidth;
	}
	
	private double rightBound(int i) {
		assert i >= 0 && i < buckets;
		return min + (i + 1) * bucketWidth;
	}
	
	private int valueToBucket(int v) {
		if (v < min)
			return -1;
		
		if (v > max)
			return buckets;
		
		for (int i = 0; i < buckets; ++i)
			if (v >= leftBound(i) && v < rightBound(i))
				return i;
		
		throw new RuntimeException("Value to bucket, bucket index not found.");
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
		// some code goes here
		int vId = valueToBucket(v);
		
		if (op == Predicate.Op.EQUALS) {
			if (vId == -1 || vId == buckets)
				return 0.;
			
			return (double) bucketValues[vId] / totalTuple;
		}
		
		if (op == Predicate.Op.GREATER_THAN) {
			if (vId == -1 || vId == buckets)
				return (vId == -1) ? 1. : 0.;
			
			double sel = (rightBound(vId) - v) * bucketValues[vId];
			for (int i = vId + 1; i < buckets; ++i)
				sel += bucketValues[i];
			return sel / totalTuple;
		}
		
		if (op == Predicate.Op.LESS_THAN) {
			if (vId == -1 || vId == buckets)
				return (vId == -1) ? 0. : 1.;
			
			double sel = (v - leftBound(vId)) * bucketValues[vId];
			for (int i = 0; i < vId; ++i)
				sel += bucketValues[i];
			return sel / totalTuple;
		}
		
		if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
			return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
		}

		if (op == Predicate.Op.LESS_THAN_OR_EQ) {
			return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
		}
		
		if (op == Predicate.Op.NOT_EQUALS) {
			return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
		}
		
		throw new RuntimeException("Unknown predicate type in estimating selectivity.");
	}
	
	/**
	 * @return the average selectivity of this histogram.
	 * <p>
	 * This is not an indispensable method to implement the basic
	 * join optimization. It may be needed if you want to
	 * implement a more efficient optimization
	 */
	public double avgSelectivity() {
		// some code goes here
		return 1.0;
	}
	
	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		// some code goes here
		return "IntHistogram toString()";
	}
}
