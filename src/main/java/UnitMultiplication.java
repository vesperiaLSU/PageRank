import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    // beta value used for teleporting weights from 'spider trap' or 'dead ends'
    private static final double beta = 0.2;

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String[] values = value.toString().trim().split("\t");
            // check for bad input
            if (values.length < 2 || values[1].trim().length() == 0) return;
            String outputKey = values[0];
            String[] pages = values[1].split(",");
            for (String page : pages) {
                context.write(new Text(outputKey), new Text(page + "=" + (double)1 / pages.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] values = value.toString().trim().split("\t");
            // check for bad input
            if (values.length < 2) return;
            context.write(new Text(values[0]), new Text(values[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            List<String> transitions = new ArrayList<String>();
            double pr = 0;
            for (Text value : values) {
                String line = value.toString().trim();
                if (line.contains("=")) {
                    transitions.add(line);
                } else {
                    pr = Integer.parseInt(line);
                }
            }

            for (String tran : transitions) {
                String[] relation = tran.trim().split("=");
                if (relation.length < 2) continue;
                String outputKey = relation[0];
                double newPr = (1 - beta) * Double.parseDouble(relation[1]) * pr + beta * pr;
                String outputValue = String.valueOf(newPr);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
