public class DerivedB extends Base {

  public char b_field;

  public Boolean overlap_field;

  public DerivedB(char b, Boolean overlap) {
    super(Type.TYPE_B);

    b_field = b;
    overlap_field = overlap;
  }
}

