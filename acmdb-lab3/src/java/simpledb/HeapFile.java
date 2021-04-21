package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        byte[] page = new byte[BufferPool.getPageSize()];
        try (RandomAccessFile f = new RandomAccessFile(this.f, "r")) {
            f.seek((long) pid.pageNumber() * BufferPool.getPageSize());
            f.read(page);
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), page);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile file = new RandomAccessFile(this.f, "rw");
        file.seek((long) page.getId().pageNumber() * BufferPool.getPageSize());
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                ArrayList<Page> ret = new ArrayList<>();
                ret.add(page);
                return ret;
            }
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);
        ArrayList<Page> ret = new ArrayList<>();
        ret.add(newPage);
        return ret;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> ret = new ArrayList<>();
        ret.add(page);
        return ret;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            Iterator<Integer> page = Collections.emptyIterator();
            Iterator<Tuple> tuple = Collections.emptyIterator();

            @Override
            public void open() throws DbException, TransactionAbortedException {
                page = IntStream.range(0, numPages()).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                while (!tuple.hasNext() && page.hasNext())
                    tuple = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), page.next()), Permissions.READ_ONLY)).iterator();
                return tuple.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                while (!tuple.hasNext() && page.hasNext())
                    tuple = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), page.next()), Permissions.READ_ONLY)).iterator();
                return tuple.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
                tuple = Collections.emptyIterator();
            }

            @Override
            public void close() {
                page = Collections.emptyIterator();
                tuple = Collections.emptyIterator();
            }
        };
    }

}

