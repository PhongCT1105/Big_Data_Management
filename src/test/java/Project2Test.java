import org.junit.Test;

public class Project2Test {
    // Change paths as needed
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";
    public final static String output = path + "output";

    @Test
    public void Requals1() {
        String[] input = new String[6];
        input[0] = path + "points.csv";
        input[1] = output + "/Requals1";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";
        input[4] = "1";
        input[5] = "False";
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
        input[3] = "10";
        input[4] = "6";
        input[5] = "False";
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
        input[3] = "10";
        input[4] = "20";
        input[5] = "True";
        try {
            kMeans.main(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
