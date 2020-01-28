import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class mergeFile {

    public static class myMapper extends Mapper<Object,Text, IntWritable,Text>{

        private final IntWritable inti=new IntWritable(1);
        private Text key=new Text();

        public void map(Object keyVal,Text val,Context context) throws IOException,InterruptedException{

            key.set(val.toString());
            context.write(inti,key);

        }

    }

    public static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text>{

        private Text res=new Text();

        public void reduce(IntWritable key,Iterable<Text> docs,Context context) throws IOException, InterruptedException {

            StringBuilder temp=new StringBuilder();
            String temporaryString="";

            for(Text smallDocs:docs)
            {
                temp=new StringBuilder(temporaryString).append(smallDocs.toString());
            }

            res.set(temp.toString());

            context.write(key,res);

        }

    }

    public static void main(String args[]) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"Merge Files");

        job.setJarByClass(mergeFile.class);
        job.setMapperClass(myMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);

        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        if(!job.waitForCompletion(true))
            System.exit(0);
        else
            System.exit(1);


    }
}
