package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId t;
    private DbIterator child;
    private boolean deleted;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
	    this.t = t;
	    this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
	    return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	    super.open();
	    child.open();
	    deleted = false;
    }

    public void close() {
        // some code goes here
	    super.close();
	    child.close();
	    deleted = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	    super.close();
	    super.open();
	    child.rewind();
	    deleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//	    if (deletedRecord == 0)
//	    	return createNumberTuple(deletedRecord++);
		   
	    if (deleted) return null;
	
	    int deletedCnt = 0;
	    while (child.hasNext()) {
		    try {
			    Database.getBufferPool().deleteTuple(t, child.next());
			    deletedCnt += 1;
		    } catch (IOException e) {
			    throw new TransactionAbortedException();
		    }
	    }
	    deleted = true;
	    return createNumberTuple(deletedCnt);
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
