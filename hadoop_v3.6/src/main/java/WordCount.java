import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    // garde ton chemin comme il marche chez toi (relatif ou absolu)
    private static final String INPUT_PATH = "input-wordCount/";
    private static final String OUTPUT_PATH = "output/wordCount-";
    private static final Logger LOG = Logger.getLogger(WordCount.class.getName());

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

    // ------------------ MAP ------------------
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static String emptyWords[] = { "" };

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // ligne brute
            String line = value.toString();

            // 1) mettre en minuscules
            line = line.toLowerCase();

            // 2) supprimer la ponctuation / caractères non lettres/chiffres
            //    tout ce qui n’est pas lettre (\p{L}) ou chiffre (\p{Nd}) devient un espace
            line = line.replaceAll("[^\\p{L}\\p{Nd}]+", " ");

            // 3) découper en mots
            String[] words = line.split("\\s+");

            if (Arrays.equals(words, emptyWords))
                return;

            // 4) ne garder que les mots de longueur > 4
            for (String word : words) {
                if (word.length() > 4) {
                    context.write(new Text(word), one);
                }
            }
        }
    }

    // ------------------ REDUCE ------------------
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // 5) n’afficher que les mots avec au moins 10 occurrences
            if (sum >= 10) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    // ------------------ MAIN ------------------
    public static void main(String[] args) throws Exception {
        System.out.println("Working dir = " + new java.io.File(".").getAbsolutePath());

        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount-filter");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}

