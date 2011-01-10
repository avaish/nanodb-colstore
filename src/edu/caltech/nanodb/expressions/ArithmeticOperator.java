package edu.caltech.nanodb.expressions;


import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;

import java.util.Collection;



/**
 * This class implements simple binary arithmetic operations.  The supported
 * operations are:
 * <ul>
 *   <li>addition, <tt>+</tt></li>
 *   <li>subtraction, <tt>-</tt></li>
 *   <li>multiplication, <tt>*</tt></li>
 *   <li>division, <tt>/</tt></li>
 *   <li>remainder, <tt>%</tt></li>
 * </ul>
 *
 * @todo Division probably should generate floating-point results on integer arguments.
 */
public class ArithmeticOperator extends Expression {

    /**
     * This enum specifies the arithmetic operations that this class can provide.
     */
    public enum Type {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        REMAINDER("%");

        /** The string representation for each operator.  Used for printing. */
        private final String stringRep;

        /** Construct a Type enum with the specified string representation. */
        Type(String rep) {
            stringRep = rep;
        }

        /** Accessor for the operator type's string representation. */
        public String stringRep() {
            return stringRep;
        }
    }


    /** The kind of comparison, such as "subtract" or "multiply." */
    Type type;

    /** The left expression in the comparison. */
    Expression leftExpr;

    /** The right expression in the comparison. */
    Expression rightExpr;



    public ArithmeticOperator(Type type, Expression lhs, Expression rhs) {
        if (type == null || lhs == null || rhs == null)
            throw new NullPointerException();

        leftExpr = lhs;
        rightExpr = rhs;

        this.type = type;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        ColumnInfo ltColInfo = leftExpr.getColumnInfo(schema);
        ColumnInfo rtColInfo = rightExpr.getColumnInfo(schema);

        SQLDataType resultSQLType = getSQLResultType(
            ltColInfo.getType().getBaseType(), rtColInfo.getType().getBaseType());

        ColumnType colType = new ColumnType(resultSQLType);
        return new ColumnInfo(colType);
    }


    private SQLDataType getSQLResultType(SQLDataType lType, SQLDataType rType) {
        // This array specifies the type-conversion sequence.  If at least one of
        // the arguments is type typeOrder[i], then both arguments are coerced to
        // that type.  (This is not entirely accurate at the moment, but is
        // perfectly sufficient for our needs.)
        SQLDataType[] typeOrder = {
            SQLDataType.NUMERIC, SQLDataType.DOUBLE, SQLDataType.FLOAT,
            SQLDataType.BIGINT, SQLDataType.INTEGER, SQLDataType.SMALLINT,
            SQLDataType.TINYINT
        };

        for (int i = 0; i < typeOrder.length; i++) {
            if (lType == typeOrder[i] || rType == typeOrder[i])
                return typeOrder[i];
        }

        // Just guess INTEGER.  Works for C...
        return SQLDataType.INTEGER;
    }


    public Object evaluate(Environment env) throws ExpressionException {
        // Evaluate the left and right subexpressions.
        Object lhsValue = leftExpr.evaluate(env);
        Object rhsValue = rightExpr.evaluate(env);

        // If either the LHS value or RHS value is NULL (represented by Java
        // null value) then the entire expression evaluates to FALSE.
        if (lhsValue == null || rhsValue == null)
            return Boolean.FALSE;

        // Coerce the values to both have the same numeric type.

        TypeConverter.Pair coerced =
            TypeConverter.coerceArithmetic(lhsValue, rhsValue);

        Object result = null;

        if (coerced.value1 instanceof Double) {
            result = evalDoubles((Double) coerced.value1, (Double) coerced.value2);
        }
        else if (coerced.value1 instanceof Float) {
            result = evalFloats((Float) coerced.value1, (Float) coerced.value2);
        }
        else if (coerced.value1 instanceof Long) {
            result = evalLongs((Long) coerced.value1, (Long) coerced.value2);
        }
        else {
            assert coerced.value1 instanceof Integer;
            result = evalIntegers((Integer) coerced.value1, (Integer) coerced.value2);
        }

        return result;
    }


    /** This helper implements the arithmetic operations for Double values. */
    private Double evalDoubles(Double aObj, Double bObj) {
        double a = aObj.doubleValue();
        double b = bObj.doubleValue();
        double result = 0;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        case DIVIDE:
            result = a / b;
            break;

        case REMAINDER:
            result = a % b;
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return new Double(result);
    }


    /** This helper implements the arithmetic operations for Float values. */
    private Float evalFloats(Float aObj, Float bObj) {
        float a = aObj.floatValue();
        float b = bObj.floatValue();
        float result = 0;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        case DIVIDE:
            result = a / b;
            break;

        case REMAINDER:
            result = a % b;
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return new Float(result);
    }


    /** This helper implements the arithmetic operations for Long values. */
    private Long evalLongs(Long aObj, Long bObj) {
        long a = aObj.longValue();
        long b = bObj.longValue();
        long result = 0;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        case DIVIDE:
            result = a / b;
            break;

        case REMAINDER:
            result = a % b;
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return new Long(result);
    }


    /** This helper implements the arithmetic operations for Integer values. */
    private Integer evalIntegers(Integer aObj, Integer bObj) {
        int a = aObj.intValue();
        int b = bObj.intValue();
        int result = 0;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        case DIVIDE:
            result = a / b;
            break;

        case REMAINDER:
            result = a % b;
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return new Integer(result);
    }


    /**
     * This method returns true if either the left or right subexpression
     * contains symbols.
     */
    public boolean hasSymbols() {
        return leftExpr.hasSymbols() || rightExpr.hasSymbols();
    }


    /**
     * Collects all symbols from the left and right subexpressions of this
     * arithmetic operation and stores them into the specified set.
     */
    public void getAllSymbols(Collection<ColumnName> symbols) {
        leftExpr.getAllSymbols(symbols);
        rightExpr.getAllSymbols(symbols);
    }


    /**
     * Returns a string representation of this arithmetic expression and its
     * subexpressions, including parentheses where necessary to specify
     * precedence.
     */
    @Override
    public String toString() {
        // Convert all of the components into string representations.
        String leftStr = leftExpr.toString();
        String rightStr = rightExpr.toString();
        String opStr = " " + type.stringRep() + " ";

        // Figure out if I need parentheses around the subexpressions.

        if (type == Type.MULTIPLY || type == Type.DIVIDE || type == Type.REMAINDER) {
            if (leftExpr instanceof ArithmeticOperator) {
                ArithmeticOperator leftOp = (ArithmeticOperator) leftExpr;
                if (leftOp.type == Type.ADD || leftOp.type == Type.SUBTRACT)
                    leftStr = "(" + leftStr + ")";
            }

            if (rightExpr instanceof ArithmeticOperator) {
                ArithmeticOperator rightOp = (ArithmeticOperator) rightExpr;
                if (rightOp.type == Type.ADD || rightOp.type == Type.SUBTRACT)
                    rightStr = "(" + rightStr + ")";
            }
        }

        return leftStr + opStr + rightStr;
    }


    /**
     * Simplifies an arithmetic expression, computing as much of the expression
     * as possible.
     */
    public Expression simplify() {
        leftExpr = leftExpr.simplify();
        rightExpr = rightExpr.simplify();

        if (!leftExpr.hasSymbols())
            leftExpr = new LiteralValue(leftExpr.evaluate());

        if (!rightExpr.hasSymbols())
            rightExpr = new LiteralValue(rightExpr.evaluate());

        if (!hasSymbols())
            return new LiteralValue(evaluate());

        return this;
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necesarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ArithmeticOperator) {
            ArithmeticOperator other = (ArithmeticOperator) obj;
            return (type.equals(other.type) &&
                    leftExpr.equals(other.leftExpr) &&
                    rightExpr.equals(other.rightExpr));
        }
        return false;
    }


    /**
     * Computes the hashcode of an Expression.  This method is used to see if
     * two expressions CAN be equal.
     */
    @Override
    public int hashCode() {
        int hash = 7;

        hash = 31 * hash + type.hashCode();

        hash = 31 * hash + leftExpr.hashCode();
        hash = 31 * hash + rightExpr.hashCode();

        return hash;
    }


    /** Creates a copy of expression. */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ArithmeticOperator expr = (ArithmeticOperator)super.clone();

        // Type is immutable, copy it.
        expr.type = this.type;

        // Clone the subexpressions
        expr.leftExpr = (Expression) leftExpr.clone();
        expr.rightExpr = (Expression) rightExpr.clone();

        return expr;
    }
}
