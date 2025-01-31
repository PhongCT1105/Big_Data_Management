import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVParser {
    private static String[] headers; // Stores column headers
    private static Map<String, Integer> headerIndexMap = new HashMap<>(); // Maps header names to indexes
    private static List<String[]> table = new ArrayList<>(); // Stores CSV data

    public static void parseCSV(String filePath) {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> allRows = reader.readAll();
            if (!allRows.isEmpty()) {
                headers = allRows.get(0); // First row = headers
                for (int i = 0; i < headers.length; i++) {
                    headerIndexMap.put(headers[i].trim(), i);
                }
                table = allRows.subList(1, allRows.size()); // Remaining rows = data
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }

    // ✅ Get all headers
    public static String[] getHeaders() {
        return headers;
    }

    // ✅ Get a row by index
    public static String[] getRow(int rowIndex) {
        return (rowIndex < table.size()) ? table.get(rowIndex) : null;
    }

    // ✅ Get a value using row index & column name
    public static String getValue(int rowIndex, String columnName) {
        Integer colIndex = headerIndexMap.get(columnName);
        String[] row = getRow(rowIndex);
        return (colIndex != null && row != null && colIndex < row.length) ? row[colIndex] : null;
    }

    // ✅ Print the table for debugging
    public static void printTable() {
        System.out.println("Headers: " + String.join(" | ", headers));
        for (String[] row : table) {
            System.out.println(String.join(" | ", row));
        }
    }
}
