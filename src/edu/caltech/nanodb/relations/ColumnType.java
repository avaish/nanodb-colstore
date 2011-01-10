package edu.caltech.nanodb.relations;

import java.util.HashMap;


/**
 * The type of a single column in a relation.  The type is composed of two
 * parts.  The first part is present in all attribute-types, and is the "base
 * type" of the attribute, such as <code>INTEGER</code>, <code>TIMESTAMP</code>,
 * or <code>CHARACTER</code>.  These values are represented by the
 * {@link SQLDataType} enumeration.  However, some of these types also require
 * additional information about their length, precision, or other details.
 * This second part is included as values in this class.  The two parts together
 * represent the type of a particular attribute.
 **/
public class ColumnType {

  /** The base SQL data-type for the attribute. **/
  private SQLDataType baseType;

  /**
   * For CHAR and VARCHAR attributes, this contains the length (or maximum
   * length) of the character-sequence.  This value must be at least 1; zero and
   * negative values are invalid.
   * <p>
   * This value is required in CHAR and VARCHAR attribute-declarations, but we
   * default to a length of 1 here.
   **/
  private int length = 1;


  /**
   * For NUMERIC attributes, this contains the precision of the number, or the
   * total number of significant digits in the number.
   * <p>
   * The default precision is {@link java.lang.Integer#MAX_VALUE}.
   **/
  private int precision = Integer.MAX_VALUE;


  /**
   * For NUMERIC attributes, this contains the scale of the number, or the
   * number of significant digits that are to the right of the decimal place.
   * <p>
   * The SQL Standard says that the default scale should be zero, which although
   * this seems a bit useless, that's what we do.
   **/
  private int scale = 0;


  /**
   * Construct a new attribute-type instance, with the specified base SQL
   * datatype.  Note that more information may be needed to complete the
   * attribute-type information, depending on the SQL datatype that is being
   * used.
   **/
  public ColumnType(SQLDataType baseType) {
    if (baseType == null)
      throw new NullPointerException("baseType cannot be null");

    this.baseType = baseType;
  }


  /**
   * Returns <code>true</code> if <code>obj</code> is another
   * <code>ColumnType</code> object with the same type information, including
   * both base type and any relevant size information.
   **/
  public boolean equals(Object obj) {

    if (obj instanceof ColumnType) {
      ColumnType c = (ColumnType) obj;
      if (baseType.equals(c.baseType)) {
        // Depending on the base type, we might need to check additional fields.
        switch (baseType) {

        case NUMERIC:
          return (scale == c.scale && precision == c.precision);

        case CHAR:
        case VARCHAR:
          return (length == c.length);

        default:
          // No other types have additional values to check.
          return true;
        }
      }
    }
    return false;
  }


  /** Returns the base datatype for the attribute. **/
  public SQLDataType getBaseType() {
    return baseType;
  }


  /**
   * Returns true if this column type supports/requires a length, or false if
   * the type does not.
   **/
  public boolean hasLength() {
    return baseType == SQLDataType.CHAR || baseType == SQLDataType.VARCHAR;
  }


  /**
   * Specify the length of the data for CHAR and VARCHAR attributes.
   *
   * @throws java.lang.IllegalStateException if the attribute's base-type is not
   *         CHAR or VARCHAR.
   * @throws java.lang.IllegalArgumentException if the specified length is zero
   *         or negative.
   **/
  public void setLength(int val) {

    if (!hasLength()) {
      throw new IllegalStateException(
        "This SQL data type does not support a length.");
    }

    if (val < 1 || val > 65535) {
      throw new IllegalArgumentException(
        "Length must be in range [1, 65535].  Got " + val);
    }

    length = val;
  }


  /**
   * Returns the length of the data for CHAR and VARCHAR attributes.
   *
   * @throws java.lang.IllegalStateException if the attribute's base-type is not
   *         CHAR or VARCHAR.
   **/
  public int getLength() {

    if (!hasLength()) {
      throw new IllegalStateException(
        "This SQL data type does not support a length.");
    }

    return length;
  }


  /**
   * Specify the precision of the data for NUMERIC attributes.
   *
   * @throws java.lang.IllegalStateException if the attribute's base-type is not
   *         NUMERIC.
   * @throws java.lang.IllegalArgumentException if the specified precision is
   *         zero or negative.
   * @throws java.lang.IllegalArgumentException if the specified precision is
   *         less than the scale.
   **/
  public void setPrecision(int val) {

    if (baseType != SQLDataType.NUMERIC) {
      throw new IllegalStateException(
        "Precision only applies to NUMERIC types.");
    }

    if (val < 1)
      throw new IllegalArgumentException("Precision must be > 0.");

    if (val < scale)
      throw new IllegalArgumentException("Precision must be at least as large as scale.");

    precision = val;
  }


  /**
   * Returns the precision of the data for NUMERIC attributes.
   *
   * @throws java.lang.IllegalStateException if the attribute's base-type is not
   *         NUMERIC.
   **/
  public int getPrecision() {
    if (baseType != SQLDataType.NUMERIC) {
      throw new IllegalStateException(
        "Precision only applies to NUMERIC types.");
    }

    return precision;
  }


  /**
   * Specify the scale of the data for NUMERIC attributes.
   *
   * @throws java.lang.IllegalStateException if the attribute's base-type is not
   *         NUMERIC.
   * @throws java.lang.IllegalArgumentException if the specified scale is
   *         negative.
   * @throws java.lang.IllegalArgumentException if the specified scale is
   *         greater than the precision.
   **/
  public void setScale(int val) {

    if (baseType != SQLDataType.NUMERIC)
      throw new IllegalStateException("Scale only applies to NUMERIC types.");

    if (val < 0)
      throw new IllegalArgumentException("Scale must be >= 0.");

    if (val > precision) {
      throw new IllegalArgumentException(
        "Precision must be at least as large as scale.");
    }

    scale = val;
  }


  /**
   * Returns the scale of the data for NUMERIC attributes.
   *
   * @throws java.lang.IllegalStateException if the attribute's base-type is not
   *         NUMERIC.
   **/
  public int getScale() {
    if (baseType != SQLDataType.NUMERIC)
      throw new IllegalStateException("Scale only applies to NUMERIC types.");

    return scale;
  }


  public String toString() {
    String result = baseType.toString();

    if (hasLength())
      result += "(" + length + ")";

    return result;
  }
}
