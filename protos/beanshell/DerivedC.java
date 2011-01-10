public class DerivedC extends Base {

  public boolean c_field;

  public String overlap_field;

  public DerivedC(boolean c, String overlap) {
    super(Type.TYPE_C);

    c_field = c;
    overlap_field = overlap;
  }
}

