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

public class EdgeListForwardIndex {

  public static class OutVertexMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String strValue = value.toString();
      int idx = strValue.indexOf("|");
      String newKey = strValue.substring(0,idx);
      String newValue = strValue.substring(idx+1);

      // Only process edges (skip headers)
      if (newKey.matches("^[0-9].*")) {
        context.write(new Text(newKey), new Text(newValue));
      }
    }
  }

  public static class AggregateEdgesReducer
       extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();

    public void setup(Context context) 
        throws IOException, InterruptedException {
      String strValue = context.getConfiguration().get("my.parameters.header");
      int idx = strValue.indexOf("|");
      String newKey = strValue.substring(0,idx);
      String newValue = strValue.substring(idx+1);

      context.write(new Text(newKey), new Text(newValue));
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
      System.err.println("Usage: edgelistforwardindex <social_network> <out>");
      System.exit(2);
    }
    conf.set("mapreduce.output.textoutputformat.separator", "|");

    String edgeTypes[] = {
//        "comment_hasCreator_person",
//        "comment_hasTag_tag",
//        "comment_isLocatedIn_place",
//        "comment_replyOf_comment",
//        "comment_replyOf_post",
//        "forum_containerOf_post",
//        "forum_hasMember_person",
//        "forum_hasModerator_person",
//        "forum_hasTag_tag",
//        "organisation_isLocatedIn_place",
//        "person_hasInterest_tag",
//        "person_isLocatedIn_place",
        "person_likes_comment",
        "person_likes_post"};
//        "person_studyAt_organisation",
//        "person_workAt_organisation",
//        "place_isPartOf_place",
//        "post_hasCreator_person",
//        "post_hasTag_tag",
//        "post_isLocatedIn_place",
//        "tagclass_isSubclassOf_tagclass",
//        "tag_hasType_tagclass"};

    String edgeTypeHeaders[] = {
//        "Comment.id|Person.id",
//        "Comment.id|Tag.id",
//        "Comment.id|Place.id",
//        "Comment.id|Comment.id",
//        "Comment.id|Post.id",
//        "Forum.id|Post.id",
//        "Forum.id|Person.id|joinDate",
//        "Forum.id|Person.id",
//        "Forum.id|Tag.id",
//        "Organisation.id|Place.id",
//        "Person.id|Tag.id",
//        "Person.id|Place.id",
        "Person.id|Comment.id|creationDate",
        "Person.id|Post.id|creationDate"};
//        "Person.id|Organisation.id|classYear",
//        "Person.id|Organisation.id|workFrom",
//        "Place.id|Place.id",
//        "Post.id|Person.id",
//        "Post.id|Tag.id",
//        "Post.id|Place.id",
//        "TagClass.id|TagClass.id",
//        "Tag.id|TagClass.id"};

    // Run a MapReduce job per edge type
    for (int i = 0; i < edgeTypes.length; i++) {
      String edgeType = edgeTypes[i];
      String header = edgeTypeHeaders[i];
      conf.set("my.parameters.header", header);
      Job job = new Job(conf, "Generate " + edgeType + " Forward Index");
      job.setJarByClass(EdgeListForwardIndex.class);
      job.setMapperClass(OutVertexMapper.class);
      job.setReducerClass(AggregateEdgesReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      String inputFileMatchPattern = otherArgs[0] + "/" + edgeType + "*.csv";
      FileInputFormat.addInputPath(job, new Path(inputFileMatchPattern));
      String outputDirectory = otherArgs[1] + "/" + edgeType;
      FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
      job.getConfiguration().set("mapreduce.output.basename", 
          edgeType + "_fidx");
      job.setNumReduceTasks(1);
      if (!job.waitForCompletion(true)) {
        System.exit(1);
      }
    }

    System.exit(0);
  }
}
