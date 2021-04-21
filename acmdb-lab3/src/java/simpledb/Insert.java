package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId t;
    private final DbIterator child;
    private final int tableId;

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
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        Type[] typeArr = new Type[1];
        typeArr[0] = Type.INT_TYPE;
        String[] nameArr = new String[1];
        return new TupleDesc(typeArr, nameArr);
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    }

    private boolean called = false;

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
        if (called) return null;
        called = true;
        int cnt = 0;
        child.open();
        while (child.hasNext()) {
            Tuple cur = child.next();
            try {
                Database.getBufferPool().insertTuple(t, tableId, cur);
                cnt++;
            } catch (IOException e) {
                throw new DbException("insert fail");
            }
        }
        child.close();
        Tuple ret = new Tuple(getTupleDesc());
        ret.setField(0, new IntField(cnt));
        return ret;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] ret = new DbIterator[1];
        setChildren(ret);
        return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        children[0] = child;
    }
}
