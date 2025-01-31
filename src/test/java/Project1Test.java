import org.junit.Test;

public class Project1Test {
    // Change path as needed!
    public final static String path = "/Users/antoski/WPI/Big_Data_Management/";

    @Test
    public void testTaskA() {
        TaskA taskA = new TaskA();
        String[] input = new String[2];
        input[0] = path + "pages.csv";
        input[1] = path + "output";
        try {
            taskA.debug(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
