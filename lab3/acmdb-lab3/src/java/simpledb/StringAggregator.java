package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupCounter;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        if (what == Op.COUNT) {
            if (groupCounter == null) {
                groupCounter = new HashMap<Field, Integer>();
            }
            groupCounter.put(groupField, groupCounter.getOrDefault(groupField, 0) + 1);
        } else {
            throw new UnsupportedOperationException("Unsupported operation: " + what);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new Operator() {

            private Iterator<Map.Entry<Field, Integer>> it;

            public void open() throws DbException, TransactionAbortedException {
                super.open();
                it = groupCounter.entrySet().iterator();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = groupCounter.entrySet().iterator();
            }

            @Override
            public void close() {
                super.close();
                it = null;
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (gbfield == NO_GROUPING) {
                    return new TupleDesc(new Type[] { Type.INT_TYPE });
                } else {
                    return new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
                }
            }

            @Override
            protected Tuple fetchNext() throws DbException, TransactionAbortedException {
                if (it != null && it.hasNext()) {
                    Map.Entry<Field, Integer> entry = it.next();
                    Tuple tuple = new Tuple(getTupleDesc());
                    if (gbfield == NO_GROUPING) {
                        tuple.setField(0, new IntField(entry.getValue()));
                    } else {
                        tuple.setField(0, entry.getKey());
                        tuple.setField(1, new IntField(entry.getValue()));
                    }
                    return tuple;
                }
                return null;
            }

            public void setChildren(DbIterator[] children) {
                // do nothing
            }

            public DbIterator[] getChildren() {
                return null;
            }
        };
    }

}
