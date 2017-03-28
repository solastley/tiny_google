/* main program for the Java Map Reduce jobs */
import java.io.IOException;

import java.util.*;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class initialize {
    private static PrintWriter indexWriter;
    private static Lemmatizer lem;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            /* must provide a path to input directory, temporary output directory,
            and final output directory */
            System.err.println("Usage: Initialize <input path> <temp_path> <output path>");
            System.exit(-1);
        }

        indexWriter = new PrintWriter("/tmp/tiny_google_index/index.txt", "UTF-8");
        lem = new Lemmatizer();

        /* -------------------------- Job 1 ---------------------------------- */

        /* initialize job 1 */
        JobConf job1 = new JobConf(initialize.class);
        job1.setJobName("Tiny Google Word Counter");

        /* set input and output key and value types for job 1 */
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        /* set mapper, combiner, reducer classes for job 1 */
        job1.setMapperClass(TGWordCountMapper.class);
        job1.setCombinerClass(TGWordCountReducer.class);
        job1.setReducerClass(TGWordCountReducer.class);

        /* set input and output formats */
        job1.setInputFormat(TextInputFormat.class);
        job1.setOutputFormat(TextOutputFormat.class);

        /* set input and output directory paths */
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        /* run job 1 */
        JobClient.runJob(job1);

        /* -------------------------- Job 2 ---------------------------------- */
        JobConf job2 = new JobConf(initialize.class);
        job2.setJobName("Tiny Google Inverted Index Creator");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(TGIndexMapper.class);
        job2.setCombinerClass(TGIndexReducer.class);
        job2.setReducerClass(TGIndexReducer.class);

        job2.setInputFormat(TextInputFormat.class);
        job2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        JobClient.runJob(job2);

        indexWriter.flush();
        indexWriter.close();
    }

    public static class TGWordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private String filename; // name of file being processed
        private final int CHUNK_SIZE = 512; // size of our file chunks in bytes

        public TGWordCountMapper() {}

        public void configure(JobConf job) {
            String [] parts = job.get("map.input.file").split("/");
            filename = parts[parts.length - 1];
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            long offset = key.get();

            int chunk = (int) (offset / CHUNK_SIZE);

            // if this line spans two chunks, must break it up
            if (offset + line.length() >= CHUNK_SIZE * (chunk + 1)) {
                int bytesLeftInChunk = (int) (CHUNK_SIZE - (offset % CHUNK_SIZE));

                // split line into parts
                String firstHalf = line.substring(0, line.length() - bytesLeftInChunk + 1);
                String secondHalf = line.substring(line.length() - bytesLeftInChunk + 1, line.length());

                // split parts into individual words
                List<String> firstProcessed = lem.lemmatize(firstHalf);
                List<String> secondProcessed = lem.lemmatize(secondHalf);

                // write values for first section of line
                for (String word : firstProcessed) {
                    if (word.length() > 0 && word != null) {
                        String outKey = word + "," + filename + "," + chunk;
                        output.collect(new Text(outKey), new IntWritable(1));
                    }
                }
                // write values for second section of line
                for (String word : firstProcessed) {
                    if (word.length() > 0 && word != null) {
                        String outKey = word + "," + filename + "," + (chunk + 1);
                        output.collect(new Text(outKey), new IntWritable(1));
                    }
                }
                /*
                 // split parts into individual words
                String [] firstSplit = firstHalf.split("[^a-zA-Z0-9]+");
                String [] secondSplit = secondHalf.split("[^a-zA-Z0-9]+");

                // write values for first section of line
                for (int i = 0; i < firstSplit.length; i++) {
                    if (firstSplit[i].length() > 0 && firstSplit[i] != null) {
                        String outKey = firstSplit[i] + "," + filename + "," + chunk;
                        output.collect(new Text(outKey), new IntWritable(1));
                    }
                }
                // write values for second section of line
                for (int i = 0; i < secondSplit.length; i++) {
                    if (secondSplit[i].length() > 0 && secondSplit[i] != null) {
                        String outKey = secondSplit[i] + "," + filename + "," + (chunk + 1);
                        output.collect(new Text(outKey), new IntWritable(1));
                    }
                }*/
            }
            // this line is contained within one chunk
            else {
                /*
                String [] lineSplit = line.split("[^a-zA-Z0-9]+");
                // write values for this line
                for (int i = 0; i < lineSplit.length; i++) {
                    if (lineSplit[i].length() > 0 && lineSplit[i] != null) {
                        String outKey = lineSplit[i] + "," + filename + "," + chunk;
                        output.collect(new Text(outKey), new IntWritable(1));
                    }
                }*/
                List<String> processed = lem.lemmatize(line);

                // write values for first section of line
                for (String word : processed) {
                    if (word.length() > 0 && word != null) {
                        String outKey = word + "," + filename + "," + chunk;
                        output.collect(new Text(outKey), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class TGWordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public TGWordCountReducer() {}

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int frequency = 0;
            for (IntWritable val; values.hasNext();) {
                values.next();
                frequency++;
            }

            output.collect(key, new IntWritable(frequency));
        }
    }

    public static class TGIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public TGIndexMapper() {}

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String [] parts = line.split("\t");

            int frequency = Integer.parseInt(parts[1]);
            String [] documentDataParts = parts[0].split(",");

            String term = documentDataParts[0];
            String bookFilename = documentDataParts[1];
            String chunk = documentDataParts[2];

            if (bookFilename != "" && chunk != "") {
                String documentData = bookFilename + "," + chunk + "," + frequency;
                output.collect(new Text(term), new Text(documentData));
            }
        }
    }

    public static class TGIndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public TGIndexReducer() {}

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Map<String, List<ChunkData>> documents = new HashMap<String, List<ChunkData>>();

            while (values.hasNext()) {
                String data = values.next().toString();
                String [] parts = data.split(",");
                String docName = parts[0];
                String chunk = parts[1];
                String freq = parts[2];

                if (!documents.containsKey(docName)) {
                    documents.put(docName, new ArrayList<ChunkData>());
                    documents.get(docName).add(new ChunkData(chunk, freq));
                }
                else {
                    documents.get(docName).add(new ChunkData(chunk, freq));
                }
            }

            StringBuilder indexValue = new StringBuilder();
            Iterator<String> iter = documents.keySet().iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                indexValue.append("/" + name + ",");
                List<ChunkData> chunkList = documents.get(name);
                for (int i = 0; i < chunkList.size(); i++) {
                    ChunkData curr = chunkList.get(i);
                    indexValue.append(curr.chunk + ":" + curr.frequency);
                    if (i != chunkList.size() - 1) {
                        indexValue.append(",");
                    }
                }
            }

            indexWriter.println(key + "\t" + indexValue.toString());
        }

        private class ChunkData {
            String chunk;
            String frequency;

            public ChunkData(String c, String f) {
                this.chunk = new String(c);
                this.frequency = new String(f);
            }
        }
        private class Lemmatizer {
            protected StanfordCoreNLP pipeline;
            private Lemmatizer(){
                Properties props;
                props = new Properties();
                props.put("annotators", "tokenize, ssplit, pos, lemma");

                this.pipeline = new StanfordCoreNLP(props);
            }
            public List<String> lemmatize(String documentText) {
                List<String> lemmas = new ArrayList<String>();
                Annotation document = new Annotation(documentText);
                // run all Annotators on this text
                this.pipeline.annotate(document);
                // Iterate over all of the sentences found
                List<CoreMap> sentences = document.get(SentencesAnnotation.class);
                for(CoreMap sentence: sentences) {
                    // Iterate over all tokens in a sentence
                    for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                        // Retrieve and add the lemma for each word into the
                        // list of lemmas
                        lemmas.add(token.get(LemmaAnnotation.class));
                    }
                }
                return lemmas;
            }
        }
    }
}
