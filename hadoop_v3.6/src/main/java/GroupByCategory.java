import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupByCategory {

    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupByCategory-";
    private static final Logger LOG = Logger.getLogger(GroupByCategory.class.getName());

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
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // séparation par virgules (CSV)
            String[] fields = line.split(",");

            // ignorer l'en-tête
            if (fields[0].equals("Row ID")) {
                return;
            }

            // sécurité : ligne trop courte = on ignore
            if (fields.length <= 17) {
                LOG.warning("Ligne ignorée (trop peu de colonnes) : " + line);
                return;
            }

            // indices pour superstore.csv
            String date     = fields[2];   // Order Date
            String category = fields[14];  // Category
            String salesStr = fields[17];  // Sales

            try {
                double sales = Double.parseDouble(salesStr);

                // clé composite : Date;Category
                outKey.set(date + ";" + category);
                outValue.set(sales);

                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
                LOG.warning("Ligne ignorée (Sales non numérique) : " + line);
            }
        }
    }

    // ===================== REDUCE =====================
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            for (DoubleWritable v : values) {
                sum += v.get();
            }

            context.write(key, new DoubleWritable(sum));
        }
    }

    // ===================== MAIN =====================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy_Date_Category");

        // types en sortie du Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // types en sortie du Reducer (fichier final)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}


/*Affiche les premières lignes :
head -5 input-groupBy/superstore.csv
 */

/*cat output/groupByCategory-1764938862/part-r-00000  Q3.2 */