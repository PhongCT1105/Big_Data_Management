import org.junit.Test;

import tasks.TaskA;
import tasks.TaskC;
import tasks.TaskE;
import tasks.TaskG;

public class Project1Test {
    // Change paths as needed!!
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";
    public final static String output = path + "output/";

    @Test
    public void basicTaskA() {
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = output + "A";
        try {
            TaskA.basic(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
        TaskG taskG = new TaskG();
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
}
