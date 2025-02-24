import org.junit.Test;

public class Project2Test {
    // Change path as needed!
    // Sandi Path
//    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";

    // Phong Path
     public final static String path = "C:/Study/CS4433/Big_Data_Management/";

    // Khoi Path
    // public final static String path = "C:/Study/CS4433/Big_Data_Management/";

    // Output Path
    public final static String output = path + "output/";

    @Test
    public void testRequals1() {
        String[] input = new String[7];
        input[0] = path + "points.csv";
        input[1] = output + "/Requals1";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";  // Number of clusters (K)
        input[4] = "1";   // Number of iterations (R)
        input[5] = "False"; // No convergence check
        input[6] = "False"; // Output only cluster centers

        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRequals6() {
        String[] input = new String[7];
        input[0] = path + "points.csv";
        input[1] = output + "/Requals6";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";
        input[4] = "6";
        input[5] = "False";
        input[6] = "False";

        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTolerance() {
        String[] input = new String[7];
        input[0] = path + "points.csv";
        input[1] = output + "/tolerance";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";
        input[4] = "20";
        input[5] = "True"; // Enable convergence check
        input[6] = "False";

        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // **New Test Cases for Task (e)**

    @Test
    public void testOutputClusterCentersOnly() {
        String[] input = new String[7];
        input[0] = path + "points.csv";
        input[1] = output + "/outputCentroids";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";
        input[4] = "10";
        input[5] = "True"; // Enable convergence check
        input[6] = "False"; // Output only cluster centers

        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testOutputClusteredDataPoints() {
        String[] input = new String[7];
        input[0] = path + "points.csv";
        input[1] = output + "/outputClusteredData";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";
        input[4] = "10";
        input[5] = "True"; // Enable convergence check
        input[6] = "True"; // Output clustered data points

        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
