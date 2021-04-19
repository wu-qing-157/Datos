package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TDItem)) return false;
            return Objects.equals(fieldType, ((TDItem) o).fieldType) && Objects.equals(fieldName, ((TDItem) o).fieldName);
        }

        @Override
        public int hashCode() {
            int result = fieldType != null ? fieldType.hashCode() : 0;
            result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
            return result;
        }
    }

    final private TDItem[] items;
    final private Map<String, Integer> nameToIdx;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.stream(items).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        items = new TDItem[typeAr.length];
        nameToIdx = new HashMap<>();
        for (int i = 0; i < typeAr.length; i++) {
            items[i] = new TDItem(typeAr[i], fieldAr[i]);
            if (fieldAr[i] != null)
                nameToIdx.put(fieldAr[i], i);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= items.length) throw new NoSuchElementException();
        return items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= items.length) throw new NoSuchElementException();
        return items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        Integer idx = nameToIdx.get(name);
        if (idx == null) throw new NoSuchElementException();
        return idx;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return Arrays.stream(items).map(item -> item.fieldType.getLen()).reduce(0, Integer::sum);
    }

    private TupleDesc(TDItem[] items) {
        this.items = items;
        nameToIdx = new HashMap<>();
        for (int i = 0; i < items.length; i++) {
            if (items[i].fieldName != null)
                nameToIdx.put(items[i].fieldName, i);
        }
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */

    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        return new TupleDesc(Stream.concat(Arrays.stream(td1.items), Arrays.stream(td2.items)).toArray(TDItem[]::new));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        return o instanceof TupleDesc && Arrays.equals(items, ((TupleDesc) o).items);
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        return Arrays.stream(items).map(item -> String.format("%s(%s)", item.fieldType, item.fieldName)).collect(Collectors.joining(", "));
    }
}
