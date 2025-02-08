import org.junit.Test;

import tasks.*;

public class Project1Test {
    // Change path as needed!
//    Sandi Path
//    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";

//    Phong Path
    public final static String path = "C:/Study/CS4433/Big_Data_Management/";

    @Test
    public void testTaskA() {
        TaskA taskA = new TaskA();
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = path + "output/A";
        try {
            taskA.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskC() {
        TaskC taskC = new TaskC();
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = path + "output/C";
        try {
            System.out.println("Input Path: " + input[0]);
            taskC.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskE() {
        TaskE taskE = new TaskE();
        String[] input = new String[2];
        input[0] = path + "access_logs.csv";
        input[1] = path + "output/E";
        try {
            System.out.println("Input Path: " + input[0]);
            taskE.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskG() {
        TaskG taskG = new TaskG();
        String[] input = new String[3];
        input[0] = path + "access_logs.csv";
        input[1] = path + "pages.csv";
        input[2] = path + "output/G";
        try {
            System.out.println("Input Path: " + input[0]);
            taskG.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskB() {
        TaskB taskB = new TaskB();
        String[] input = new String[3];
        input[0] = path + "access_logs.csv";
        input[1] = path + "pages.csv";
        input[2] = path + "output/B";
        try {
            System.out.println("Input Path: " + input[0]);
            taskB.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskD() {
        TaskD taskD = new TaskD();
        String[] input = new String[3];
        input[0] = path + "friends.csv";  // Friendship file
        input[1] = path + "output/D";     // Output directory
        input[2] = path + "pages.csv";    // Page owners file
        try {
            System.out.println("Input Path: " + input[0]);
            taskD.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskF() {
        TaskF taskF = new TaskF();
        String[] input = new String[4];
        input[0] = path + "friends.csv";      // Friendship file
        input[1] = path + "access_logs.csv";  // Access logs file
        input[2] = path + "output/F";         // Output directory
        input[3] = path + "pages.csv";        // Person details file
        try {
            System.out.println("Input Path: " + input[0]);
            taskF.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTaskH() {
        TaskH taskH = new TaskH();
        String[] input = new String[4];
        input[0] = path + "friends.csv";           // Friend relationships file
        input[1] = path + "pages.csv";             // People details file
        input[2] = path + "output/intermediate";   // Intermediate output directory for Job 1
        input[3] = path + "output/popular";        // Final output directory for Job 2
        try {
            System.out.println("Testing TaskH with input: " + input[0]);
            taskH.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
