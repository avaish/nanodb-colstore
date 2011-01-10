package edu.caltech.nanodb.storage;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * This class provides page-level access to a database file, which contains
 * some kind of data utilized in a database system.  The class provides no
 * caching whatsoever; page requests will always read the file, and page
 * writes are always written to the disk.  Furthermore, writes are always
 * synchronized to disk, so that the data on disk actually reflects the
 * contents of the file in memory.
 * <p>
 * Obviously, this class is not intended to provide an efficient interface to
 * the table data stored on disk.  That must be provided by higher-level
 * mechanisms layered on top of this class.
 * <p>
 * This class may be utilized for many different kinds of database files.
 * Here is an example of the kinds of data that might be stored in files:
 * <ul>
 *   <li>Tuples in a database table.</li>
 *   <li>Table indexes in sequential, B-tree, or some other format.</li>
 *   <li>Recovery logs.</li>
 *   <li>Checkpoint files.</li>
 * </ul>
 *
 * @see RandomAccessFile
 */
public class DBFile {
    /** The minimum page size is 512 bytes. */
    public static final int MIN_PAGESIZE = 512;

    /** The maximum page size is 64K bytes. */
    public static final int MAX_PAGESIZE = 65536;

    /** The default page size is 8K bytes. */
    public static final int DEFAULT_PAGESIZE = 8192;


    /** The actual data file on disk. */
    private File dataFile;


    /** The type of the data file. */
    private DBFileType type;


    /**
     * This is the size of pages that are read and written to the data file.
     * This value will be a power of two, between the minimum and maximum
     * page sizes.
     */
    private int pageSize;


    /** The file data is accessed via this variable. */
    private RandomAccessFile fileContents;


    /**
     * This static helper method returns true if the specified page size is
     * valid; i.e. it must be within the minimum and maximum page sizes, and
     * it must be a power of two.
     *
     * @param pageSize the page-size to test for validity
     *
     * @return true if the specified page-size is valid, or false otherwise.
     */
    public static boolean isValidPageSize(int pageSize) {
      // The first line ensures that the page size is in the proper range,
      // and the second line ensures that it is a power of two.
      return (pageSize >= MIN_PAGESIZE && pageSize <= MAX_PAGESIZE) &&
             ((pageSize & (pageSize - 1)) == 0);
    }


    /**
     * This static helper method checks the specified page-size with
     * {@link #isValidPageSize}, and if the size is not valid then an
     * {@link IllegalArgumentException} runtime exception is thrown.  The method
     * is intended for checking arguments.
     *
     * @param pageSize the page-size to test for validity
     *
     * @throws IllegalArgumentException if the specified page-size is invalid.
     */
    public static void checkValidPageSize(int pageSize) {
        if (!isValidPageSize(pageSize)) {
            throw new IllegalArgumentException(String.format(
                "Page size must be a power of two, in the range [%d, %d].  Got %d",
                MIN_PAGESIZE, MAX_PAGESIZE, pageSize));
        }
    }


    /**
     * Given a valid page size, this method returns the base-2 logarithm of the
     * page size for storing in a data file.  For example,
     * <tt>encodePageSize(512)</tt> will return 9.
     *
     * @param pageSize the page-size to encode
     *
     * @return the base-2 logarithm of the page size
     *
     * @throws IllegalArgumentException if the specified page-size is invalid.
     */
    public static int encodePageSize(int pageSize) {
        checkValidPageSize(pageSize);

        int encoded = 0;
        while (pageSize > 1) {
            pageSize >>= 1;
            encoded++;
        }

        return encoded;
    }


    /**
     * Given the base-2 logarithm of a page size, this method returns the actual
     * page size.  For example, <tt>decodePageSize(9)</tt> will return 512.
     *
     * @param encoded the encoded page-size
     *
     * @return the actual page size, computed as 2<sup><em>encoded</em></sup>.
     *
     * @throws IllegalArgumentException if the resulting page-size is invalid.
     */
    public static int decodePageSize(int encoded) {
        int pageSize = 0;

        if (encoded > 0)
            pageSize = 1 << encoded;

        checkValidPageSize(pageSize);

        return pageSize;
    }



    /**
     * Constructs a new object from the specified information, and opens the
     * backing data-file as well.
     *
     * @param dataFile the actual file containing the data
     * @param type the type of the data file
     * @param pageSize the page-size of the data file
     *
     * @throws IllegalArgumentException if the page size is not valid.
     * @throws IOException if some other IO error occurs
     */
    public DBFile(File dataFile, DBFileType type, int pageSize) throws IOException {
        this(dataFile, type, pageSize, new RandomAccessFile(dataFile, "rw"));
    }


    /**
     * Constructs a new object from the specified information and the previously
     * opened data-file.
     *
     * @param dataFile the actual file containing the data
     * @param type the type of the data file
     * @param pageSize the page-size of the data file
     * @param fileContents an already opened {@link RandomAccessFile} to use for
     *        accessing the data file's contents
     *
     * @throws IllegalArgumentException if the page size is not valid.
     * @throws IOException if some other IO error occurs
     */
    public DBFile(File dataFile, DBFileType type, int pageSize,
        RandomAccessFile fileContents) throws IOException {

        if (dataFile == null || type == null || fileContents == null)
            throw new NullPointerException();

        checkValidPageSize(pageSize);

        this.dataFile = dataFile;
        this.type = type;
        this.pageSize = pageSize;
        this.fileContents = fileContents;

        // TODO:  Verify that the file's stored page-size and type match the
        //        values we were passed!

        // Check to make sure the file contains a whole number of pages.
        long fileSize = fileContents.length();
        if (fileSize % (long) pageSize != 0) {
            // Maybe handle this someday by extending the file to have a whole
            // page at the end, but this is definitely the more conservative
            // approach.
            throw new IllegalStateException("Data file " + dataFile +
                " ends with a partial page!");
        }
    }


    /**
     * Returns <tt>true</tt> if <tt>obj</tt> is an instance of <tt>DBFile</tt>
     * with the same backing file.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DBFile) {
            DBFile other = (DBFile) obj;
            return dataFile.equals(other.getDataFile());
        }

        return false;
    }


    /** Returns a hash-code based on the filename of the backing file. */
    @Override
    public int hashCode() {
      return dataFile.hashCode();
    }


    @Override
    public String toString() {
        return dataFile.getName();
    }

    
    /**
     * Returns the actual file that holds the data on the disk.
     *
     * @return the {@link File} object that actually holds the data on disk.
     */
    public File getDataFile() {
        return dataFile;
    }


    /**
     * Returns the type of this data file.
     *
     * @return the enumerated value indicating the file's type.
     */
    public DBFileType getType() {
        return type;
    }


    /**
     * Returns the page size for this database file, in bytes.
     *
     * @return the page size for this database file, in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }


    /**
     * Returns the {@link RandomAccessFile} for accessing the data file's
     * contents.
     *
     * @return the {@link RandomAccessFile} for accessing the data file's
     *         contents.
     */
    public RandomAccessFile getFileContents() {
        return fileContents;
    }
}
