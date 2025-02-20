import org.junit.Test;

public class Project2Test {
    // Change paths as needed
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";
    public final static String output = path + "output";

    @Test
    public void kMeans() {
        String[] input = new String[5];
        input[0] = path + "points.csv";
        input[1] = output;
        input[2] = path + "k_seeds.csv";
        input[3] = "15";
        input[4] = "3";
        try {
            System.out.println("Input Path: " + input[0]);
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
