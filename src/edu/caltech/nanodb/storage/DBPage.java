package edu.caltech.nanodb.storage;


import java.io.UnsupportedEncodingException;

import java.util.Arrays;

import org.apache.log4j.Logger;


/**
 * This class represents a single page in a database file.  The page's
 * (zero-based) index in the file, and whether the page has been changed in
 * memory, are tracked by the object.
 * <p>
 * Database pages do not provide any locking mechanisms to guard against
 * concurrent access.  Locking must be managed at a level above what this class
 * provides.
 * <p>
 * The class provides methods to read and write a wide range of data types.
 * Multibyte values are stored in big-endian format, with the most significant
 * byte (MSB) stored at the lowest index, and the least significant byte (LSB)
 * stored at the highest index.  (This is also the network byte order specified
 * by the Internet Protocol.)
 *
 * @see PageReader
 * @see PageWriter
 */
public class DBPage {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DBPage.class);


    /** A reference to the database file that this page is from. */
    private DBFile dbFile;

    /**
     * The page number in the table file.  A value of 0 is the first page
     * in the file.
     */
    private int pageNo;


    /** This flag is true if this page has been modified in memory. */
    private boolean dirty;


    /** The actual data for the table-page. */
    private byte[] pageData;


    /**
     * Constructs a new, empty table-page for the specified table file.
     * Note that the page data is not loaded into the object; that must be
     * done in a separate step.
     *
     * @param dbFile The database file that this page is contained within.
     *
     * @param pageNo The page number within the database file.
     *
     * @throws NullPointerException if <tt>dbFile</tt> is <tt>null</tt>
     * 
     * @throws IllegalArgumentException if <tt>pageNo</tt> is negative
     */
    public DBPage(DBFile dbFile, int pageNo) {
        if (dbFile == null)
            throw new NullPointerException("dbFile must not be null");

        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be >= 0 (got " +
                pageNo + ")");
        }

        this.dbFile = dbFile;
        this.pageNo = pageNo;
        dirty = false;

        // Allocate the space for the page data.
        pageData = new byte[dbFile.getPageSize()];
    }


    /**
     * Returns the database file that this page is contained within.
     *
     * @return the database file that this page is contained within.
     */
    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns true if this page is from the specified database file.  This
     * function simply uses {@link DBFile#equals} for the comparison.
     *
     * @param databaseFile the database file to examine this page for membership
     *
     * @return true if the specified database file is the same as this DB file.
     */
    public boolean isFromDBFile(DBFile databaseFile) {
        return dbFile.equals(databaseFile);
    }


    /**
     * Returns the page-number of this database page.
     *
     * @return the page-number of this database page
     */
    public int getPageNo() {
        return pageNo;
    }


    /**
     * Returns the page size in bytes.
     *
     * @return the page-size in bytes
     */
    public int getPageSize() {
        return pageData.length;
    }


    /**
     * Returns the byte-array of the page's data.  <b>Note that if any changes
     * are made to the page's data, the dirty-flag must be updated
     * appropriately or else the data will not be written back to the file.</b>
     *
     * @return a byte-array containing the page's data
     */
    public byte[] getPageData() {
        return pageData;
    }


    /**
     * Returns true if the page's data has been changed in memory; false
     * otherwise.
     *
     * @return true if the page's data has been changed in memory
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Sets the dirty flag to true or false, indicating whether the page's data
     * has or has not been changed in memory.
     *
     * @param dirty the dirty flag; true if the page's data is dirty, or false
     *        otherwise
     */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /*=============================*/
    /* TYPED DATA ACCESS FUNCTIONS */
    /*=============================*/


    /**
     * Read a sequence of bytes into the provided byte-array, starting with
     * the specified offset, and reading the specified number of bytes.
     *
     * @param position the starting index within the page to start reading data
     *
     * @param b the destination buffer to save the data into
     *
     * @param off the starting offset to save data to in the destination buffer
     *
     * @param len the number of bytes to transfer to the destination buffer
     */
    public void read(int position, byte[] b, int off, int len) {
        System.arraycopy(pageData, position, b, off, len);
    }


    /**
     * Read a sequence of bytes into the provided byte-array.  The entire
     * array is filled from start to end.
     *
     * @param position the starting index within the page to start reading data
     *
     * @param b the destination buffer to save the data into
     */
    public void read(int position, byte[] b) {
        read(position, b, 0, b.length);
    }


    /**
     * Write a sequence of bytes from a byte-array into the page, starting with
     * the specified offset in the buffer, and writing the specified number of
     * bytes.
     *
     * @param position the starting index within the page to start writing data
     *
     * @param b the source buffer to read the data from
     *
     * @param off the starting offset to read data from the source buffer
     *
     * @param len the number of bytes to transfer from the source buffer
     */
    public void write(int position, byte[] b, int off, int len) {
        System.arraycopy(b, off, pageData, position, len);
        setDirty(true);
    }


    /**
     * Write a sequence of bytes from a byte-array into the page.  The entire
     * contents of the array is written to the page.
     *
     * @param position the starting index within the page to start writing data
     *
     * @param b the source buffer to read the data from
     */
    public void write(int position, byte[] b) {
        // Use the version of write() with extra args.
        write(position, b, 0, b.length);
    }


    /**
     * Reads and returns a Boolean value from the specified position.  The
     * Boolean value is encoded as a single byte; a zero value is interpreted
     * as <tt>false</tt>, and a nonzero value is interpreted as <tt>true</tt>.
     *
     * @param position the starting location in the page to start reading the
     *        value from
     *
     * @return the Boolean value
     */
    public boolean readBoolean(int position) {
        return (pageData[position] != 0);
    }

    /**
     * Writes a Boolean value to the specified position.  The Boolean value is
     * encoded as a single byte; <tt>false</tt> is written as 0, and
     * <tt>true</tt> is written as 1.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the Boolean value
     */
    public void writeBoolean(int position, boolean value) {
        pageData[position] = (byte) (value ? 1 : 0);
        setDirty(true);
    }


    /**
     * Reads and returns a signed byte from the specified position.
     *
     * @param position the location in the page to read the value from
     *
     * @return the signed byte value
     */
    public byte readByte(int position) {
        return pageData[position];
    }

    /**
     * Writes a (signed or unsigned) byte to the specified position.  The byte
     * value is specified as an integer for the sake of convenience
     * (specifically to avoid having to cast an argument to a byte value), but
     * the input is also truncated down to the low 8 bits.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the byte value
     */
    public void writeByte(int position, int value) {
        pageData[position] = (byte) value;
        setDirty(true);
    }


    /**
     * Reads and returns an unsigned byte from the specified position.  The
     * value is returned as an <tt>int</tt> whose value will be between
     * 0 and 255, inclusive.
     *
     * @param position the location in the page to read the value from
     *
     * @return the unsigned byte value, as an integer
     */
    public int readUnsignedByte(int position) {
        return pageData[position] & 0xFF;
    }


    /**
     * Reads and returns an unsigned short from the specified position.  The
     * value is returned as an <tt>int</tt> whose value will be between
     * 0 and 65535, inclusive.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the unsigned short value, as an integer
     */
    public int readUnsignedShort(int position) {
        int value = ((pageData[position++] & 0xFF) <<  8)
                  | ((pageData[position  ] & 0xFF)      );

        return value;
    }

    /**
     * Reads and returns a signed short from the specified position.  The
     * value is returned as a <tt>short</tt>.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the signed short value
     */
    public short readShort(int position) {
        // Don't chop off high-order bits.  When byte is cast to int, the sign
        // will be extended, so if original byte is negative, the resulting
        // int will be too.
        int value = ((pageData[position++]       ) <<  8)
                  | ((pageData[position  ] & 0xFF)      );

        return (short) value;
    }

    /**
     * Writes a (signed or unsigned) short to the specified position.  The short
     * value is specified as an integer for the sake of convenience
     * (specifically to avoid having to cast an argument to a short value), but
     * the input is also truncated down to the low 16 bits.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the byte value
     */
    public void writeShort(int position, int value) {
        pageData[position++] = (byte) (0xFF & (value >> 8));
        pageData[position  ] = (byte) (0xFF &  value);

        setDirty(true);
    }


    /**
     * Reads and returns a two-byte char value from the specified position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the char value
     */
    public char readChar(int position)
    {
        // NOTE:  Exactly like readShort(), but result is cast to a different
        // type.

        // Don't chop off high-order bits.  When byte is cast to int, the sign will
        // be extended, so if original byte is negative, so will resulting int.
        int value = ((pageData[position++]       ) <<  8)
                  | ((pageData[position  ] & 0xFF)      );

        return (char) value;
    }

    /**
     * Writes a char to the specified position.  The char value is specified as
     * an integer for the sake of convenience (specifically to avoid having to
     * cast an argument to a char value), but the input is also truncated down
     * to the low 16 bits.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the char value
     */
    public void writeChar(int position, int value) {
        // Implementation is identical to writeShort()...
        writeShort(position, value);
    }


    /**
     * Reads and returns a 4-byte unsigned integer value from the specified
     * position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the unsigned integer value, as a long
     */
    public long readUnsignedInt(int position) {
        long value = ((pageData[position++] & 0xFF) << 24)
                   | ((pageData[position++] & 0xFF) << 16)
                   | ((pageData[position++] & 0xFF) <<  8)
                   | ((pageData[position  ] & 0xFF)      );

        return value;
    }


    /**
     * Reads and returns a 4-byte integer value from the specified position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the signed int value
     */
    public int readInt(int position) {
        int value = ((pageData[position++] & 0xFF) << 24)
                  | ((pageData[position++] & 0xFF) << 16)
                  | ((pageData[position++] & 0xFF) <<  8)
                  | ((pageData[position  ] & 0xFF)      );

        return value;
    }

    /**
     * Writes a 4-byte integer to the specified position.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the 4-byte integer value
     */
    public void writeInt(int position, int value) {
        pageData[position++] = (byte) (0xFF & (value >> 24));
        pageData[position++] = (byte) (0xFF & (value >> 16));
        pageData[position++] = (byte) (0xFF & (value >>  8));
        pageData[position  ] = (byte) (0xFF &  value);

        setDirty(true);
    }


    /**
     * Reads and returns an 8-byte long integer value from the specified
     * position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the signed long value
     */
    public long readLong(int position) {
        long value = ((long) (pageData[position++] & 0xFF) << 56)
                   | ((long) (pageData[position++] & 0xFF) << 48)
                   | ((long) (pageData[position++] & 0xFF) << 40)
                   | ((long) (pageData[position++] & 0xFF) << 32)
                   | ((long) (pageData[position++] & 0xFF) << 24)
                   | ((long) (pageData[position++] & 0xFF) << 16)
                   | ((long) (pageData[position++] & 0xFF) <<  8)
                   | ((long) (pageData[position  ] & 0xFF)      );

        return value;
    }

    /**
     * Writes an 8-byte long integer to the specified position.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the 8-byte long integer value
     */
    public void writeLong(int position, long value) {
        pageData[position++] = (byte) (0xFF & (value >> 56));
        pageData[position++] = (byte) (0xFF & (value >> 48));
        pageData[position++] = (byte) (0xFF & (value >> 40));
        pageData[position++] = (byte) (0xFF & (value >> 32));
        pageData[position++] = (byte) (0xFF & (value >> 24));
        pageData[position++] = (byte) (0xFF & (value >> 16));
        pageData[position++] = (byte) (0xFF & (value >>  8));
        pageData[position  ] = (byte) (0xFF &  value);

        setDirty(true);
    }


    public float readFloat(int position) {
        return Float.intBitsToFloat(readInt(position));
    }


    public void writeFloat(int position, float value) {
        writeInt(position, Float.floatToIntBits(value));
    }


    public double readDouble(int position) {
        return Double.longBitsToDouble(readLong(position));
    }


    public void writeDouble(int position, double value) {
        writeLong(position, Double.doubleToLongBits(value));
    }


    /**
     * This method reads and returns a variable-length string whose maximum
     * length is 255 bytes.  The string is expected to be in US-ASCII
     * encoding, so multibyte characters are not supported.
     * <p>
     * The string's data format is expected to be a single unsigned byte
     * <em>b</em> specifying the string's length, followed by <em>b</em> more
     * bytes consisting of the string value itself.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return a string object containing the stored value, up to a maximum of
     *         255 characters in length
     */
    public String readVarString255(int position) {
        int len = readUnsignedByte(position++);

        String str = null;

        try {
            str = new String(pageData, position, len, "US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }

    /**
     * This method stores a variable-length string whose maximum length is
     * 255 bytes.  The string is expected to be in US-ASCII encoding, so
     * multibyte characters are not supported.
     * <p>
     * The string is stored as a single unsigned byte <em>b</em> specifying the
     * string's length, followed by <em>b</em> more bytes consisting of the
     * string value itself.
     *
     * @param position the location in the page to start writing the value to
     *
     * @param value the string object containing the data to store
     *
     * @throws NullPointerException if <tt>value</tt> is <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than
     *         255 characters
     */
    public void writeVarString255(int position, String value) {
        byte[] bytes;

        try {
            bytes = value.getBytes("US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (bytes.length > 255)
            throw new IllegalArgumentException("value must be 255 bytes or less");

        // These functions set the dirty flag.
        writeByte(position, bytes.length);
        write(position + 1, bytes);
    }


    /**
     * This method reads and returns a variable-length string whose maximum
     * length is 65535 bytes.  The string is expected to be in US-ASCII
     * encoding, so multibyte characters are not supported.
     * <p>
     * The string's data format is expected to be a single unsigned short (two
     * bytes) <em>s</em> specifying the string's length, followed by <em>s</em>
     * more bytes consisting of the string value itself.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return a string object containing the stored value, up to a maximum of
     *         65535 characters in length
     */
    public String readVarString65535(int position) {
        int len = readUnsignedShort(position);
        position += 2;

        String str = null;

        try {
            str = new String(pageData, position, len, "US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }

    /**
     * This method stores a variable-length string whose maximum length is
     * 65535 bytes.  The string is expected to be in US-ASCII encoding, so
     * multibyte characters are not supported.
     * <p>
     * The string is stored as a single unsigned short <em>s</em> specifying the
     * string's length, followed by <em>s</em> more bytes consisting of the
     * string value itself.
     *
     * @param position the location in the page to start writing the value to
     *
     * @param value the string object containing the data to store
     *
     * @throws NullPointerException if <tt>value</tt> is <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than
     *         65535 characters
     */
    public void writeVarString65535(int position, String value) {
        byte[] bytes;

        try {
            bytes = value.getBytes("US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (bytes.length > 65535)
            throw new IllegalArgumentException("value must be 65535 bytes or less");

        // These functions set the dirty flag.
        writeShort(position, bytes.length);
        write(position + 2, bytes);
    }


    /**
     * This method reads and returns a string whose length is fixed at a
     * constant size.  The string is expected to be in US-ASCII encoding, so
     * multibyte characters are not supported.
     * <p>
     * Strings shorter than the specified length are padded with 0 bytes at the end of the string, and
     * this padding is removed when the string is read.
     *
     *
     * The string's characters are stored starting with the specified position.
     * If the string is shorter than the fixed length then the data is expected
     * to be terminated with a <tt>\\u0000</tt> (i.e. <tt>NUL</tt>) value.  (If
     * the string is exactly the given length then no string terminator is
     * expected.)  <b>The implication of this storage format is that embedded
     * <tt>NUL</tt> characters are not allowed with this storage format.</b>
     *
     * @param position the location in the page to start reading the value from
     *
     * @param len the length of the fixed-size string
     *
     * @return a string object containing the stored value, up to a maximum of
     *         <tt>len</tt> characters in length
     */
    public String readFixedSizeString(int position, int len) {
        String str = null;

        // Fixed-size strings are padded with 0-bytes, so trim these off the
        // end of the string value.
        while (len > 0 && pageData[position + len - 1] == 0)
            len--;

        try {
            str = new String(pageData, position, len, "US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }

    /**
     * This method stores a string whose length is fixed at a constant size.
     * The string is expected to be in US-ASCII encoding, so multibyte
     * characters are not supported.
     * <p>
     * The string's characters are stored starting with the specified position.
     * If the string is shorter than the fixed length then the data is padded
     * with <tt>\\u0000</tt> (i.e. <tt>NUL</tt>) values.  If the string is
     * exactly the given length then no string terminator is stored.  <b>The
     * implication of this storage format is that embedded <tt>NUL</tt>
     * characters are not allowed with this storage format.</b>
     *
     * @param position the location in the page to start writing the value to
     *
     * @param value the string object containing the data to store
     *
     * @param len the number of bytes used to store the string field
     *
     * @throws NullPointerException if <tt>value</tt> is <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than
     *         <tt>len</tt> characters
     */
    public void writeFixedSizeString(int position, String value, int len) {
        byte[] bytes;

        try {
            bytes = value.getBytes("US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (bytes.length > len) {
            throw new IllegalArgumentException("value must be " + len +
                " bytes or less");
        }

        // This function sets the dirty flag.
        write(position, bytes);

        // Zero out the rest of the fixed-size string value.
        Arrays.fill(pageData, position + bytes.length, position + len, (byte) 0);
    }
}
