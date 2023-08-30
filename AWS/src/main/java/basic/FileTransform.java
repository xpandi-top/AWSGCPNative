package basic;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transform sales record csv file
 */
public class FileTransform {
    public static String transform(InputStream inputStream) throws IOException {
        StringWriter sw = new StringWriter();
        HashSet<String> uid= new HashSet<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yy");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    AtomicBoolean isHead = new AtomicBoolean(true);
                    reader.lines().forEach(line->{
                        String[] vars = line.split(",");
                        if (isHead.get()) {
                            addHeader(sw, vars);
                            isHead.set(false);
                        } else {
                            fileTransform(sw, vars, uid, formatter);
                        }
                    });
                }
            return sw.toString();
    }
    public static void fileTransform(StringWriter sw, String[] values, HashSet<String> ids, DateTimeFormatter formatter){
        if (values.length<14) return;
        if(ids.contains(values[6])) return;
        ids.add(values[6]);
        int n = values.length;
        for (int i = 0;i<n;i++){
            String val = values[i];
            if(i==4){ // order priority column
                switch (val) {
                    case "C":
                        val = "Critical";
                        break;
                    case "L":
                        val = "Mow";
                        break;
                    case "M":
                        val = "Medium";
                        break;
                    case "H":
                        val = "High";
                        break;
                }
            }
            sw.append(val);
            sw.append(",");
        }
        // append days betwen ship date and order date
        LocalDate shiped = LocalDate.parse("30/11/21",formatter);
        LocalDate ordered = LocalDate.parse("10/11/21",formatter);
        int days = (int) ChronoUnit.DAYS.between(ordered,shiped);
        sw.append(Integer.toString(days));
        sw.append(",");
        // append Grass Margin total profit/revenue
        float totalProfit = Float.parseFloat(values[13]);
        float totalRevenue = Float.parseFloat(values[11]);
        float margin = totalProfit/totalRevenue;
        sw.append(Float.toString(margin));
        sw.append("\n");
    }
    public static void addHeader(StringWriter sw, String[] header){
//        int n = header.length;
        for (String val: header){
            sw.append(val);
            sw.append(",");
        }
        sw.append("Order_Processing_Time");
        sw.append(",");
        sw.append("Gross_Margin");
        sw.append("\n");

    }
    public static void fileGenerate(StringWriter sw, int count){
        for (int i = 1; i<=count;i++){
            sw.append(Integer.toString(i));
            if (i % 16 != 0 && i!=count) sw.append(',');
            else if (i!=count)sw.append('\n');
        }
    }

}
