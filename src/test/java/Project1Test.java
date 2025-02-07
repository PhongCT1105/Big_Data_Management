import org.junit.Test;

import tasks.TaskA;
import tasks.TaskC;
import tasks.TaskE;
import tasks.TaskG;

public class Project1Test {
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/"; // Change path as needed!
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
            TaskA.maponly(input);
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
}
