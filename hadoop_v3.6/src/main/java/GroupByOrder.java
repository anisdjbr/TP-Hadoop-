import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

public class GroupByOrder {

    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupByOrder-";
    private static final Logger LOG = Logger.getLogger(GroupByOrder.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    // ===================== MAP =====================
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split(",");

            // ignorer l'en-tête
            if (fields[0].equals("Row ID")) {
                return;
            }

            if (fields.length <= 18) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            // Order ID, Product ID, Quantity
            String orderId   = fields[1];
            String productId = fields[13];
            String qtyStr    = fields[18];

            // clé : OrderID
            outKey.set(orderId);
            // valeur : productId;qty
            outValue.set(productId + ";" + qtyStr);

            context.write(outKey, outValue);
        }
    }

    // ===================== REDUCE =====================
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashSet<String> products = new HashSet<>();
            int totalQty = 0;

            for (Text v : values) {
                String[] parts = v.toString().split(";");
                if (parts.length < 2) {
                    continue;
                }

                String productId = parts[0];
                String qtyStr    = parts[1];

                products.add(productId);

                try {
                    int q = Integer.parseInt(qtyStr);
                    totalQty += q;
                } catch (NumberFormatException e) {
                    // on ignore cette quantité
                }
            }

            int distinctProducts = products.size();

            // valeur = "nbProduitsDistincts\tnbTotalExemplaires"
            Text outValue = new Text(distinctProducts + "\t" + totalQty);
            context.write(key, outValue);
        }
    }

    // ===================== MAIN =====================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy_Order_Stats");

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // types en sortie du Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // types en sortie du Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(
                job,
                new Path(OUTPUT_PATH + Instant.now().getEpochSecond())
        );

        job.waitForCompletion(true);
    }
}

/*Affiche les premières lignes :
head -5 input-groupBy/superstore.csv
 */

/*cat output/groupByOrder-1764938862/part-r-00000  Q3.2 */