import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class MergeFile {
    public enum ExampleEnums { NUMBER_OF_SPACES , NUMBER_OF_VOWELS }

    public static class myMapper extends Mapper<Object,Text, NullWritable,Text> {



        private NullWritable init = NullWritable.get();
        private Text key = new Text();

        public void map(Object keyVal, Text val, Context context) throws IOException,InterruptedException{

            String textValue = val.toString();

            for(int i = 0; i<textValue.length(); i++)
            {
                if(textValue.charAt(i) == ' ')
                    context.getCounter(ExampleEnums.NUMBER_OF_SPACES).increment(1);
                else if(isVowel(textValue.charAt(i)))
                    context.getCounter(ExampleEnums.NUMBER_OF_VOWELS).increment(1);
            }

            key.set(val.toString());
            context.write(init,key);

        }

        private boolean isVowel(char character) {
            character = Character.toLowerCase(character);
            return character == 'a' || character == 'e' || character == 'i' || character == 'o' || character == 'u';
        }

    }

    public static class MyReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

        private Text res = new Text();
        NullWritable finKey = NullWritable.get();

        public void reduce(NullWritable key,Iterable<Text> docs,Context context) throws IOException, InterruptedException {

            StringBuilder temp = new StringBuilder();

            for(Text smallDocs:docs)
            {
                temp = temp.append(smallDocs.toString());
                temp = temp.append("\n");
            }

            res.set(temp.toString());

            context.write(finKey,res);

        }

    }

    public static void main(String args[]) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"Merge Files");

        job.setJarByClass(MergeFile.class);
        job.setMapperClass(myMapper.class);
        //job.setNumReduceTasks(0);
        //job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        //Counters



        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.out.println("Where is this ?");


        if(!job.waitForCompletion(true))
            {
                System.out.println("SPACES" + " : " + job.getCounters().findCounter(ExampleEnums.NUMBER_OF_SPACES).getValue());
                System.out.println("VOWELS" + " : " + job.getCounters().findCounter(ExampleEnums.NUMBER_OF_VOWELS).getValue());

                System.exit(0);
            }
        else
            System.exit(1);




    }
}
