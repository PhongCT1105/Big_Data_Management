import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    public void Requals1() {
        String[] input = new String[6];
        input[0] = path + "points.csv";
        input[1] = output + "/Requals1";
        input[2] = path + "k_seeds.csv";
        input[3] = "10";        // K value
        input[4] = "1";         // R = 1 (single iteration)
        input[5] = "False";     // No convergence check
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
        input[3] = "10";        // K value
        input[4] = "6";         // R = 6 iterations
        input[5] = "False";     // No convergence check
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
        input[3] = "10";        // K value
        input[4] = "20";        // Maximum iterations R = 20
        input[5] = "True";      // Enable convergence checking (FinalReducer should be run)
        try {
            kMeans.main(input);

            // Instead of assuming "iter19", we choose the iteration directory with the highest iteration number.
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path baseOutputPath = new Path(output + "/tolerance");

            FileStatus[] statuses = fs.listStatus(baseOutputPath);
            int maxIter = -1;
            Path finalIterationPath = null;
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    String folderName = status.getPath().getName();
                    if (folderName.startsWith("iter")) {
                        try {
                            int iterNum = Integer.parseInt(folderName.substring(4));
                            if (iterNum > maxIter) {
                                maxIter = iterNum;
                                finalIterationPath = status.getPath();
                            }
                        } catch (NumberFormatException e) {
                            // Ignore non-matching folder names.
                        }
                    }
                }
            }

            assertTrue("No iteration directory found under: " + baseOutputPath, finalIterationPath != null);

            // Now check that the final iteration directory contains the multiple outputs:
            // one file name starting with "centers" and one starting with "clustered".
            FileStatus[] finalFiles = fs.listStatus(finalIterationPath);
            boolean foundCenters = false;
            boolean foundClustered = false;
            for (FileStatus fileStatus : finalFiles) {
                String name = fileStatus.getPath().getName();
                if (name.startsWith("centers")) {
                    foundCenters = true;
                }
                if (name.startsWith("clustered")) {
                    foundClustered = true;
                }
            }
            assertTrue("Final output should contain centers output.", foundCenters);
            assertTrue("Final output should contain clustered output.", foundClustered);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
