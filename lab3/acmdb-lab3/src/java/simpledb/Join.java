package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc td;
    private Tuple t1;
    private Tuple t2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.child1.open();
        this.child2.open();
        super.open();
        this.t1 = null;
        this.t2 = null;
    }

    public void close() {
        // some code goes here
        this.child1.close();
        this.child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child1.rewind();
        this.child2.rewind();
        this.t1 = null;
        this.t2 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (t1 != null && this.child2.hasNext()) {
            t2 = this.child2.next();
            if (!this.p.filter(t1, t2)) {
                return fetchNext();
            }
            Tuple t = new Tuple(this.td);
            int i = 0;
            for (int j = 0; j < t1.getTupleDesc().numFields(); j++) {
                t.setField(i++, t1.getField(j));
            }
            for (int j = 0; j < t2.getTupleDesc().numFields(); j++) {
                t.setField(i++, t2.getField(j));
            }
            return t;
        } else if (this.child1.hasNext()) {
            t1 = this.child1.next();
            this.child2.rewind();
            return fetchNext();
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}