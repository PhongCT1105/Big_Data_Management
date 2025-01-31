public class Main {
    public static void main(String[] args) {
        String filePath = "pages.csv"; // Path to your CSV file

        // ✅ Parse CSV file
        CSVParser.parseCSV(filePath);

        // ✅ Print headers
        System.out.println("Headers: " + String.join(" | ", CSVParser.getHeaders()));

        // ✅ Print entire table
        CSVParser.printTable();

        // ✅ Access a specific row
        System.out.println("Row 1: " + String.join(" | ", CSVParser.getRow(1)));

        // ✅ Access a specific value using column name
        System.out.println("First row, Name: " + CSVParser.getValue(0, "PersonID"));
        System.out.println("Second row, Department: " + CSVParser.getValue(1, "Department"));
    }
}
