package net.ellitron.ldbc.snb.datagen;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Takes person_knows_person edge files as input and generates edges files that
 * contain those edges plus their dual (reversing the order of the vertices in
 * the pair). Given a source vertex, the resulting file has a complete listing
 * of that source vertex's neighbors contiguous in the file.
 */
public class KnowsRelationDualGenerator {

  public static class EdgeMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\|");
      String oldValue = tokens[1];
      String newValue = tokens[0];

      // Only process edges (skip headers)
      if (newValue.matches("^[0-9].*")) {
        for (int i = 2; i < tokens.length; i++) {
          oldValue = oldValue + "|" + tokens[i];
          newValue = newValue + "|" + tokens[i];
        }
        context.write(new Text(tokens[0]), new Text(oldValue));
        context.write(new Text(tokens[1]), new Text(newValue));
      }
    }
  }

  public static class EdgeReducer
       extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();

    public void setup(Context context) 
        throws IOException, InterruptedException {
      System.out.println("header: " + context.getConfiguration().get("my.parameters.header"));
      String[] tokens = 
          context.getConfiguration().get("my.parameters.header").split("\\|");
      String newValue = tokens[0];
      for (int i = 2; i < tokens.length; i++) {
        newValue = newValue + "|" + tokens[i];
      }
      System.out.println(String.format("Writing %s %s", new Text(tokens[1]), new Text(newValue)));
      context.write(new Text(tokens[1]), new Text(newValue));
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
        context.write(key, val);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: KnowsRelationDualGenerator <social_network> <out>");
      System.exit(2);
    }
    conf.set("mapreduce.output.textoutputformat.separator", "|");

    String edgeTypes[] = {"person_knows_person"};
    
    String edgeTypeHeaders[] = {"Person.id|Person.id|creationDate"};

    // Run a MapReduce job per edge type
    for (int i = 0; i < edgeTypes.length; i++) {
      String edgeType = edgeTypes[i];
      String header = edgeTypeHeaders[i];
      conf.set("my.parameters.header", header);
      Job job = new Job(conf, "Generate " + edgeType + " Dual Edges");
      job.setJarByClass(KnowsRelationDualGenerator.class);
      job.setMapperClass(EdgeMapper.class);
      job.setReducerClass(EdgeReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      String inputFileMatchPattern = otherArgs[0] + "/" + edgeType + "*.csv";
      FileInputFormat.addInputPath(job, new Path(inputFileMatchPattern));
      String outputDirectory = otherArgs[1] + "/" + edgeType;
      FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
      job.getConfiguration().set("mapreduce.output.basename", edgeType);
      job.setNumReduceTasks(1);
      if (!job.waitForCompletion(true)) {
        System.exit(1);
      }
    }

    System.exit(0);
  }
}
