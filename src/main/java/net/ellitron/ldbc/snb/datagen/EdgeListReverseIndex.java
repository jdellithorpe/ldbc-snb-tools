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

public class EdgeListReverseIndex {

  public static class InVertexMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\|");
      String newValue = tokens[0];
      for (int i = 2; i < tokens.length; i++) {
        newValue = newValue + "|" + tokens[i];
      }
      context.write(new Text(tokens[1]), new Text(newValue));
    }
  }

  public static class AggregateEdgesReducer
       extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();

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
      System.err.println("Usage: edgelistreverseindex <social_network> <out>");
      System.exit(2);
    }
    conf.set("mapreduce.output.textoutputformat.separator", "|");

    String edgeTypes[] = {
        "comment_hasCreator_person",
        "comment_hasTag_tag",
        "comment_isLocatedIn_place",
        "comment_replyOf_comment",
        "comment_replyOf_post",
        "forum_containerOf_post",
        "forum_hasMember_person",
        "forum_hasModerator_person",
        "forum_hasTag_tag",
        "organisation_isLocatedIn_place",
        "person_hasInterest_tag",
        "person_isLocatedIn_place",
        "person_likes_comment",
        "person_likes_post",
        "person_studyAt_organisation",
        "person_workAt_organisation",
        "place_isPartOf_place",
        "post_hasCreator_person",
        "post_hasTag_tag",
        "post_isLocatedIn_place",
        "tagclass_isSubclassOf_tagclass",
        "tag_hasType_tagclass"};

    // Run a MapReduce job per edge type
    for (String edgeType : edgeTypes) {
      Job job = new Job(conf, "Generate " + edgeType + " Reverse Index");
      job.setJarByClass(EdgeListReverseIndex.class);
      job.setMapperClass(InVertexMapper.class);
      job.setCombinerClass(AggregateEdgesReducer.class);
      job.setReducerClass(AggregateEdgesReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      String inputFileMatchPattern = otherArgs[0] + "/" + edgeType + "*.csv";
      FileInputFormat.addInputPath(job, new Path(inputFileMatchPattern));
      String outputDirectory = otherArgs[1] + "/" + edgeType;
      FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
      job.getConfiguration().set("mapreduce.output.basename", 
          edgeType + "_ridx");
      job.setNumReduceTasks(9);
      if (!job.waitForCompletion(true)) {
        System.exit(1);
      }
      
    }

    System.exit(0);
  }
}
