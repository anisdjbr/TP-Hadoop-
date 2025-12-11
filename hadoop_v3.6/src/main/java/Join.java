import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Join {
    private static final String CUSTOMERS_PATH = "input-join/customers.tbl";
    private static final String ORDERS_PATH = "input-join/orders.tbl";
    private static final String OUTPUT_PATH = "output/join-";
    private static final Logger LOG = Logger.getLogger(Join.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");
        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (Exception e) {
            System.exit(1);
        }
    }

    // Mapper pour les clients
    public static class MapCustomers extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split("\\|");

            if (fields.length >= 2) {
                String customerId = fields[0].trim();
                String customerName = fields[1].trim();

                // Émettre : <customerId, C|customerName>
                // Le "C" indique que c'est un client
                context.write(new Text(customerId), new Text("C|" + customerName));
                LOG.info("Customer: " + customerId + " = " + customerName);
            }
        }
    }

    // Mapper pour les commandes
    public static class MapOrders extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split("\\|");

            if (fields.length >= 9) {
                String orderId = fields[0].trim();
                String customerId = fields[1].trim();
                String orderComment = fields[8].trim();

                // Émettre : <customerId, O|orderId|orderComment>
                // Le "O" indique que c'est une commande
                context.write(new Text(customerId), new Text("O|" + orderId + "|" + orderComment));
                LOG.info("Order: " + customerId + " - Order " + orderId + ": " + orderComment);
            }
        }
    }

    // Reducer pour effectuer la jointure
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Stocker les clients et commandes dans des listes
            List<String> customers = new ArrayList<>();
            List<String> orders = new ArrayList<>();

            // Copier les valeurs de l'itérateur dans les arrays
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("C|")) {
                    customers.add(value.substring(2)); // Enlever le "C|"
                } else if (value.startsWith("O|")) {
                    orders.add(value.substring(2)); // Enlever le "O|"
                }
            }

            // Effectuer la jointure avec deux boucles imbriquées
            for (String customer : customers) {
                for (String order : orders) {
                    // order contient : orderId|orderComment
                    String[] orderParts = order.split("\\|", 2);
                    String orderId = orderParts.length > 0 ? orderParts[0] : "";
                    String orderComment = orderParts.length > 1 ? orderParts[1] : "";

                    // Émettre le couple (customerName, orderComment)
                    String result = customer + " | OrderID: " + orderId + " | Comment: " + orderComment;
                    context.write(new Text(customer), new Text(orderComment));

                    LOG.info("Join: " + customer + " -- " + orderComment);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "CustomerOrderJoin");
        job.setJarByClass(Join.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // IMPORTANT : MultipleInputs pour utiliser deux mappers différents
        MultipleInputs.addInputPath(job, new Path(CUSTOMERS_PATH), TextInputFormat.class, MapCustomers.class);
        MultipleInputs.addInputPath(job, new Path(ORDERS_PATH), TextInputFormat.class, MapOrders.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}