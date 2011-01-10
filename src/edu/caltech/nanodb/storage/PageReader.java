package edu.caltech.nanodb.storage;


/**
 * This class facilitates sequences of read operations against a single
 * {@link DBPage} object, by providing "position" state that is also updated
 * after each read is performed.  All read operations call through to the
 * <code>DBPage</code>'s interface.
 *
 * @see PageWriter
 * @see java.io.DataInput
 **/
public class PageReader {

  /** The page that the reader will read from. **/
  protected DBPage dbPage;

  /** The current position in the page where reads will occur from. **/
  protected int position;


  public PageReader(DBPage dbPage) {
    if (dbPage == null)
      throw new NullPointerException("dbPage");

    this.dbPage = dbPage;
    position = 0;
  }


  /**
   * Returns the current location in the page where the next operation will
   * start from.
   **/
  public int getPosition() {
    return position;
  }


  /**
   * Sets the location in the page where the next operation will start from.
   **/
  public void setPosition(int position) {
    if (position < 0 || position >= dbPage.getPageSize()) {
      throw new IllegalArgumentException("position must be in range [0," +
        dbPage.getPageSize() + ") (got " + position + ")");
    }

    this.position = position;
  }


  /**
   * Move the current position by <code>n</code> bytes.  A negative value of
   * <code>n</code> will move the position backward.
   **/
  public void movePosition(int n) {
    if (position + n > dbPage.getPageSize())
      throw new IllegalArgumentException("can't move position past page end");
    else if (position + n < 0)
      throw new IllegalArgumentException("can't move position past page start");

    position += n;
  }


  /**
   * Read a sequence of bytes into the provided byte-array, starting with the
   * specified offset, and reading the specified number of bytes.
   **/
  public void read(byte[] b, int off, int len) {
    dbPage.read(position, b, off, len);
    position += len;
  }


  /**
   * Read a sequence of bytes into the provided byte-array.  The entire array is
   * filled from start to end.
   **/
  public void read(byte[] b) {
    read(b, 0, b.length);
  }


  /**
   * Reads and returns a Boolean value from the current position.  A zero value
   * is interpreted as <code>false</code>, and a nonzero value is interpreted
   * as <code>true</code>.
   **/
  public boolean readBoolean() {
    return dbPage.readBoolean(position++);
  }

  /** Reads and returns a signed byte from the current position. **/
  public byte readByte() {
    return dbPage.readByte(position++);
  }

  /**
   * Reads and returns an unsigned byte from the current position.  The value is
   * returned as an <code>int</code> whose value will be between 0 and 255,
   * inclusive.
   **/
  public int readUnsignedByte() {
    return dbPage.readUnsignedByte(position++);
  }


  /**
   * Reads and returns an unsigned short from the current position.  The value
   * is returned as an <code>int</code> whose value will be between 0 and 65535,
   * inclusive.
   **/
  public int readUnsignedShort() {
    int value = dbPage.readUnsignedShort(position);
    position += 2;

    return value;
  }


  /** Reads and returns a signed short from the current position. **/
  public short readShort() {
    short value = dbPage.readShort(position);
    position += 2;

    return value;
  }


  /** Reads and returns a two-byte char value from the current position. **/
  public char readChar() {
    char value = dbPage.readChar(position);
    position += 2;

    return value;
  }


  /**
   * Reads and returns a four-byte unsigned integer value from the current
   * position.
   **/
  public long readUnsignedInt() {
    long value = dbPage.readUnsignedInt(position);
    position += 4;

    return value;
  }


  /** Reads and returns a four-byte integer value from the current position. **/
  public int readInt() {
    int value = dbPage.readInt(position);
    position += 4;

    return value;
  }

  /**
   * Reads and returns an eight-byte long integer value from the current
   * position.
   **/
  public long readLong() {
    long value = dbPage.readLong(position);
    position += 8;

    return value;
  }

  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }


  /**
   * This method reads and returns a variable-length string whose maximum length
   * is 255 bytes.  The string is expected to be in US-ASCII encoding, so
   * multibyte characters are not supported.
   * <p>
   * The string's data format is expected to be a single unsigned byte
   * <em>b</em> specifying the string's length, followed by <em>b</em> more
   * bytes consisting of the string value itself.
   **/
  public String readVarString255() {
    String val = dbPage.readVarString255(position);
    position += (1 + val.length());
    return val;
  }


  /**
   * This method reads and returns a variable-length string whose maximum length
   * is 65535 bytes.  The string is expected to be in US-ASCII encoding, so
   * multibyte characters are not supported.
   * <p>
   * The string's data format is expected to be a single unsigned short (two
   * bytes) <em>s</em> specifying the string's length, followed by <em>s</em>
   * more bytes consisting of the string value itself.
   **/
  public String readVarString65535() {
    String val = dbPage.readVarString65535(position);
    position += (2 + val.length());
    return val;
  }


  /**
   * This method reads and returns a string whose length is fixed at a consant
   * size.  The string is expected to be in US-ASCII encoding, so multibyte
   * characters are not supported.
   * <p>
   * Shorter strings are padded with 0 bytes at the end of the string, and this
   * padding is removed when the string is read.  Thus, the actual string read
   * may be shorter than the specified length, but the number of bytes the
   * string's value takes in the page is exactly the specified length.
   **/
  public String readFixedSizeString(int len) {
    String val = dbPage.readFixedSizeString(position, len);
    position += len;
    return val;
  }
}
