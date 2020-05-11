package simpledb;

import com.sun.xml.internal.ws.assembler.dev.TubelineAssemblyContextUpdater;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private boolean inserted;
    
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
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
	    Type[] types = new Type[1];
	    types[0] = Type.INT_TYPE;
	    return new TupleDesc(types);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	    super.open();
	    child.open();
	    inserted = false;
    }

    public void close() {
        // some code goes here
	    super.close();
	    child.close();
	    inserted = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	    super.close();
	    super.open();
	    child.rewind();
	    inserted = false;
    }

	private Tuple createNumberTuple(int n) {
		// create 1-field tuple.
		Type[] types = new Type[1];
		types[0] = Type.INT_TYPE;
		TupleDesc desc = new TupleDesc(types);
		Tuple tuple = new Tuple(desc);
		tuple.setField(0, new IntField(n));
		return tuple;
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
        // some code goes here
//	    if (insertedRecord == 0)
//		    return createNumberTuple(insertedRecord++);
		
	    if (inserted) return null;
	    
	    int insertedCnt = 0;
	    while (child.hasNext()) {
		    try {
			    Database.getBufferPool().insertTuple(t, tableId, child.next());
			    insertedCnt += 1;
		    } catch (IOException e) {
			    throw new TransactionAbortedException();
		    }
	    }
	    inserted = true;
	    return createNumberTuple(insertedCnt);
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
	    DbIterator[] children = new DbIterator[1];
	    children[0] = child;
	    return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	    assert children.length == 1;
	    child = children[0];
    }
}
