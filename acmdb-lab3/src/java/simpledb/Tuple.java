package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc schema;
    private RecordId recordId;
    private ArrayList<Field> tupleField = new ArrayList<>();

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        schema = td;

        // add clear slot when init tuple
        for (int i = 0; i < td.numFields(); ++i)
            tupleField.add(null);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        assert schema.numFields() > i;
        assert schema.getFieldType(i) == f.getType();

        tupleField.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        assert schema.numFields() > i;

        return tupleField.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String strTuple = "";

        for (int i = 0; i < schema.numFields(); ++i) {
            if (i != 0)
                strTuple += "\t";
            strTuple += tupleField.get(i).toString();
        }
        return strTuple;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return tupleField.iterator();
    }

    /**
     * reset the TupleDesc of the tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        schema = td;
        tupleField.clear();
        // add clear slot when init tuple
        for (int i = 0; i < td.numFields(); ++i)
            tupleField.add(null);
    }
}
