public class Base {

  public enum Type {
    TYPE_A,
    TYPE_B,
    TYPE_C
  }


  public Type type;


  public Base child;


  public Base(Type t) {
    if (t == null)
      throw new NullPointerException();

    type = t;
  }
}

