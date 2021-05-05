package simpledb;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final DbIterator child;
    private final int afield, gfield;
    private Aggregator.Op aop;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        return gfield == -1 ? null : "groupVal";
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return "aggregateVal";
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    private DbIterator iterator;

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        child.open();
        switch (child.getTupleDesc().getFieldType(afield)) {
            case INT_TYPE:
                IntegerAggregator integerAggregator = new IntegerAggregator(gfield, gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield), afield, aop);
                while (child.hasNext()) integerAggregator.mergeTupleIntoGroup(child.next());
                iterator = integerAggregator.iterator();
                break;
            case STRING_TYPE:
                StringAggregator stringAggregator = new StringAggregator(gfield, gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield), afield, aop);
                while (child.hasNext()) stringAggregator.mergeTupleIntoGroup(child.next());
                iterator = stringAggregator.iterator();
                break;
        }
        child.close();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext() ? iterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (gfield == -1) {
            Type[] tdType = new Type[1];
            tdType[0] = Type.INT_TYPE;
            String[] tdName = new String[1];
            return new TupleDesc(tdType, tdName);
        } else {
            Type[] tdType = new Type[2];
            tdType[0] = child.getTupleDesc().getFieldType(gfield);
            tdType[1] = Type.INT_TYPE;
            String[] tdName = new String[2];
            tdName[0] = child.getTupleDesc().getFieldName(gfield);
            return new TupleDesc(tdType, tdName);
        }
    }

    public void close() {
        super.close();
        iterator.close();
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
