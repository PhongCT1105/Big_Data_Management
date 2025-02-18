import org.junit.Test;

import tasks.*;

public class Project1Test {
    // Change path as needed!
    // Sandi Path
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";

    // Phong Path
    // public final static String path = "C:/Study/CS4433/Big_Data_Management/";
    public final static String output = path + "output/";

    @Test
    public void optimizedTaskA() {
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = output + "optimized-A";
        try {
            TaskA.optimized(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void basicTaskC() {
        TaskC taskC = new TaskC();
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = output + "C";
        try {
            System.out.println("Input Path: " + input[0]);
            taskC.basic(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void optimizedTaskC() {
        TaskC taskC = new TaskC();
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = output + "optimized-C";
        try {
            System.out.println("Input Path: " + input[0]);
            taskC.optimized(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void basicTaskE() {
        TaskE taskE = new TaskE();
        String[] input = new String[2];
        input[0] = path + "access_logs.csv";
        input[1] = output + "E";
        try {
            System.out.println("Input Path: " + input[0]);
            taskE.basic(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void optimizedTaskE() {
        TaskE taskE = new TaskE();
        String[] input = new String[2];
        input[0] = path + "access_logs.csv";
        input[1] = output + "optimized-E";
        try {
            System.out.println("Input Path: " + input[0]);
            taskE.optimized(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void basicTaskG() {
        TaskG taskG = new TaskG();
        String[] input = new String[3];
        input[0] = path + "access_logs.csv";
        input[1] = path + "pages.csv";
        input[2] = output + "G";
        try {
            System.out.println("Input Path: " + input[0]);
            taskG.basic(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void optimizedTaskG() {
        GOptimized taskG = new GOptimized();
        String[] input = new String[3];
        input[0] = path + "access_logs.csv";
        input[1] = path + "pages.csv";
        input[2] = output + "optimized-G";
        try {
            System.out.println("Input Path: " + input[0]);
            taskG.optimized(input);
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
        input[2] = output + "B";
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
        input[0] = path + "friends.csv"; // Friendship file
        input[1] = output + "D"; // Output directory
        input[2] = path + "pages.csv"; // Page owners file
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
        input[0] = path + "friends.csv"; // Friendship file
        input[1] = path + "access_logs.csv"; // Access logs file
        input[2] = output + "F";
        ; // Output directory
        input[3] = path + "pages.csv"; // Person details file
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
        input[0] = path + "friends.csv"; // Friend relationships file
        input[1] = path + "pages.csv"; // People details file
        input[2] = output + "intermediate"; // Intermediate output directory for Job 1
        input[3] = output + "popular"; // Final output directory for Job 2
        try {
            System.out.println("Testing TaskH with input: " + input[0]);
            taskH.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
