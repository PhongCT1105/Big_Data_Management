import org.junit.Test;

public class Project2Test {
    // Change paths as needed
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";
    public final static String output = path + "output/";

    @Test
    public void kMeans() {
        String[] input = new String[4];
        input[0] = path + "points.csv";
        input[1] = output + "kMeans";
        input[2] = "3";
        input[3] = "1";
        try {
            System.out.println("Input Path: " + input[0]);
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
