import org.junit.Test;

public class Project2Test {
    // Change path as needed!
    // Sandi Path
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";

    // Phong Path
    // public final static String path = "C:/Study/CS4433/Big_Data_Management/";

    // Khoi Path
    // public final static String path = "C:/Study/CS4433/Big_Data_Management/";

    // Output Path
    public final static String output = path + "output/";

    @Test
    public void Requals1() {
        String[] input = new String[6];
        input[0] = path + "points.csv";
        input[1] = output + "/Requals1";
        input[2] = path + "k_seeds.csv";
        input[3] = "10"; // Number of clusters (K)
        input[4] = "1"; // Number of iterations (R)
        input[5] = "False"; // No convergence check
        input[6] = "False"; // Output only cluster centers

        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Requals6() {
        String[] input = new String[6];
        input[0] = path + "points.csv";
        input[1] = output + "/Requals6";
        input[2] = path + "k_seeds.csv";
        input[3] = "10"; // K value
        input[4] = "6"; // R = 6 iterations
        input[5] = "False"; // No convergence check
        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tolerance() {
        String[] input = new String[6];
        input[0] = path + "points.csv";
        input[1] = output + "/tolerance";
        input[2] = path + "k_seeds.csv";
        input[3] = "10"; // K value
        input[4] = "20"; // Maximum iterations R = 20
        input[5] = "True"; // Enable convergence checking (FinalReducer should be run)
        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
