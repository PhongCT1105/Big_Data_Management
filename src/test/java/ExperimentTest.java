import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.style.Styler;

/**
 * A JUnit test class to run multiple kMeans experiments, measure execution time,
 * save results to CSV, and plot them with XChart.
 */
public class ExperimentTest {

    // Adjust paths as needed for your environment
    public static final String BASE_PATH = "C:/Study/CS4433/Big_Data_Management/";
    public static final String INPUT = BASE_PATH + "points.csv";
    public static final String SEEDS = BASE_PATH + "k_seeds.csv";
    public static final String OUTPUT_BASE = BASE_PATH + "output/experiment/";

    // A simple container for experiment results
    private static class ExperimentResult {
        int k;
        int r;
        boolean convergence;
        long durationMs;  // Execution time in milliseconds

        public ExperimentResult(int k, int r, boolean convergence, long durationMs) {
            this.k = k;
            this.r = r;
            this.convergence = convergence;
            this.durationMs = durationMs;
        }
    }

    @Test
    public void runExperimentsAndPlot() {
        // Define the parameter ranges for K, R, and convergence
        int[] kValues = {10, 20, 50};
        int[] rValues = {1, 6, 20};
        boolean[] convergenceFlags = {false, true};

        // A list to hold all experiment results
        List<ExperimentResult> results = new ArrayList<>();

        // 1. Run experiments for each (K, R, Convergence) combination
        for (int k : kValues) {
            for (int r : rValues) {
                for (boolean conv : convergenceFlags) {
                    // If R=1, having convergence check doesn't really make sense, skip it
                    if (r == 1 && conv) {
                        continue;
                    }
                    // Build an output directory for each run
                    String outputDir = OUTPUT_BASE + "K" + k + "_R" + r + "_conv" + conv;

                    // Prepare the arguments to pass to kMeans.main()
                    String[] args = new String[6];
                    args[0] = INPUT;
                    args[1] = outputDir;
                    args[2] = SEEDS;
                    args[3] = String.valueOf(k);
                    args[4] = String.valueOf(r);
                    args[5] = String.valueOf(conv);

                    System.out.println("Running kMeans with K=" + k + ", R=" + r + ", Convergence=" + conv);

                    // Measure start time
                    long startTime = System.currentTimeMillis();

                    // Run the job
                    try {
                        kMeans.main(args);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // Compute duration
                    long duration = System.currentTimeMillis() - startTime;
                    System.out.println("Duration: " + duration + " ms");

                    // Store the result
                    results.add(new ExperimentResult(k, r, conv, duration));
                }
            }
        }

        // 2. Write results to CSV
        writeResultsToCSV(results, OUTPUT_BASE + "experiment_results.csv");

        // 3. Create a simple plot: "Execution Time vs. K" for each R (with conv=false)
        for (int r : rValues) {
            List<Integer> xKValues = new ArrayList<>();
            List<Long> yTimes = new ArrayList<>();

            // Collect data points from results
            for (ExperimentResult er : results) {
                if (er.r == r && !er.convergence) {
                    xKValues.add(er.k);
                    yTimes.add(er.durationMs);
                }
            }

            // If there's no data (e.g., R=1 with conv=true was skipped), continue
            if (xKValues.isEmpty()) {
                continue;
            }

            // Convert to double[] so that addSeries(...) works
            double[] xData = xKValues.stream().mapToDouble(Double::valueOf).toArray();
            double[] yData = yTimes.stream().mapToDouble(Double::valueOf).toArray();

            // Create a chart
            XYChart chart = new XYChartBuilder()
                    .width(800).height(600)
                    .title("Execution Time vs. K (R=" + r + ", Convergence=false)")
                    .xAxisTitle("K")
                    .yAxisTitle("Time (ms)")
                    .build();

            // Customize chart appearance
            chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);

            // Add a series
            chart.addSeries("ExecutionTime", xData, yData);

            // Save the chart as a PNG
            try {
                String chartPath = OUTPUT_BASE + "ExecTime_R" + r + "_convFalse.png";
                BitmapEncoder.saveBitmap(chart, chartPath, BitmapFormat.PNG);
                System.out.println("Saved chart to " + chartPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Writes experiment results to a CSV file with columns:
     * K, R, Convergence, Duration(ms)
     */
    private void writeResultsToCSV(List<ExperimentResult> results, String filePath) {
        File csvFile = new File(filePath);
        try (PrintWriter pw = new PrintWriter(csvFile)) {
            pw.println("K,R,Convergence,Duration(ms)");
            for (ExperimentResult er : results) {
                pw.printf("%d,%d,%b,%d%n", er.k, er.r, er.convergence, er.durationMs);
            }
            System.out.println("Wrote experiment results to: " + filePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
