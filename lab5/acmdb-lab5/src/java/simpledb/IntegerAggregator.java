package simpledb;

import java.util.*;

import javax.swing.text.html.HTMLDocument.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupAggregator;
    private Map<Field, Integer> groupCounter;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what == Op.COUNT) {
            this.groupCounter = new HashMap<Field, Integer>();
        } else if (what == Op.AVG) {
            this.groupAggregator = new HashMap<Field, Integer>();
            this.groupCounter = new HashMap<Field, Integer>();
        } else {
            this.groupAggregator = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        int aggregateField = ((IntField) tup.getField(afield)).getValue();
        if (what == Op.COUNT) {
            if (groupCounter.containsKey(groupField)) {
                groupCounter.put(groupField, groupCounter.get(groupField) + 1);
            } else {
                groupCounter.put(groupField, 1);
            }
        } else if (what == Op.AVG) {
            if (groupAggregator.containsKey(groupField)) {
                groupAggregator.put(groupField, groupAggregator.get(groupField) + aggregateField);
                groupCounter.put(groupField, groupCounter.get(groupField) + 1);
            } else {
                groupAggregator.put(groupField, aggregateField);
                groupCounter.put(groupField, 1);
            }
        } else {
            if (groupAggregator.containsKey(groupField)) {
                switch (what) {
                    case MIN:
                        groupAggregator.put(groupField, Math.min(groupAggregator.get(groupField), aggregateField));
                        break;
                    case MAX:
                        groupAggregator.put(groupField, Math.max(groupAggregator.get(groupField), aggregateField));
                        break;
                    case SUM:
                        groupAggregator.put(groupField, groupAggregator.get(groupField) + aggregateField);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation");
                }
            } else {
                groupAggregator.put(groupField, aggregateField);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        }
        return new Operator() {
            private java.util.Iterator<Map.Entry<Field, Integer>> iterator = null;

            public void open() throws DbException, TransactionAbortedException {
                super.open();
                if (what == Op.COUNT) {
                    iterator = groupCounter.entrySet().iterator();
                } else {
                    iterator = groupAggregator.entrySet().iterator();
                }
            }

            public void close() {
                super.close();
                iterator = null;
            }

            public void rewind() throws DbException, TransactionAbortedException {
                if (what == Op.COUNT) {
                    iterator = groupCounter.entrySet().iterator();
                } else {
                    iterator = groupAggregator.entrySet().iterator();
                }
            }

            public TupleDesc getTupleDesc() {
                return td;
            }

            public Tuple fetchNext() {
                if (iterator == null) {
                    return null;
                }
                if (iterator.hasNext()) {
                    Map.Entry<Field, Integer> entry = iterator.next();
                    Tuple tuple = new Tuple(td);
                    IntField field = what == Op.AVG ? new IntField(entry.getValue() / groupCounter.get(entry.getKey()))
                            : new IntField(entry.getValue());
                    if (gbfield == NO_GROUPING) {
                        tuple.setField(0, field);
                    } else {
                        tuple.setField(0, entry.getKey());
                        tuple.setField(1, field);
                    }
                    return tuple;
                }
                return null;
            }

            public void setChildren(DbIterator[] children) {
                return;
            }

            public DbIterator[] getChildren() {
                return null;
            }
        };
    }

}
