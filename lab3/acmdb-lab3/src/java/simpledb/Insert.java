package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private boolean done = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);
        if (!td.equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.done = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        this.done = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        this.done = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (done) {
            return null;
        }
        done = true;
        int count = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        Tuple result = new Tuple(td);
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (children.length > 0) {
            child = children[0];
        }
    }
}
