import org.junit.Test;

import tasks.TaskA;
import tasks.TaskC;

public class Project1Test {
    // Change path as needed!
    public final static String path = "/Users/antoski/WPI/CS4433/Big_Data_Management/";

    @Test
    public void testTaskA() {
        TaskA taskA = new TaskA();
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = path + "A_output";
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
        input[1] = path + "C_output";
        try {
            System.out.println("Input Path: " + input[0]);
            taskC.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
