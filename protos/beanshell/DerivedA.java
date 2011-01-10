public class DerivedA extends Base {

  public String a_field;

  public int overlap_field;

  public DerivedA(String a, int overlap) {
    super(Type.TYPE_A);

    a_field = a;
    overlap_field = overlap;
  }
}

