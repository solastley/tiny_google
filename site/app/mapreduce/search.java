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

public class search {
    final int NUMBER_OF_BOOKS = 15;

    private static ArrayList<String> keywords;
    private static PrintWriter resultWriter;
    private static Lemmatizer lem;

    public static void main(String[] args) throws Exception {
        /* args[0] is input path, args[1] is output path */
        String inputDir = args[0];
        String outputDir = args[1];
        /* the rest are keywords to search for */
        lem = new Lemmatizer();
        String query = "";
        for (int i = 2; i < args.length; i++) {
            query = query + " " + args[i];
        }
        ArrayList<String> keywords = lem.lemmatize(query);

        resultWriter = new PrintWriter("/tmp/tiny_google_results/results.txt", "UTF-8");

        /* initialize job */
        JobConf job1 = new JobConf(search.class);
        job1.setJobName("Tiny Google Index Search");

        /* set input and output key and value types for job */
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        /* set mapper class and specify no reducer */
        job1.setMapperClass(TGSearchMapper.class);
        job1.setNumReduceTasks(0);

        /* set input and output formats */
        job1.setInputFormat(TextInputFormat.class);
        job1.setOutputFormat(TextOutputFormat.class);

        /* set input and output directory paths */
        FileInputFormat.setInputPaths(job1, new Path(inputDir));
        FileOutputFormat.setOutputPath(job1, new Path(outputDir));

        /* run job 1 */
        JobClient.runJob(job1);

        resultWriter.flush();
        resultWriter.close();
    }

    public static class TGSearchMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public TGSearchMapper() {}

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String [] parts = line.split("\t");
            String term = parts[0];
            List<DocData> counts = new ArrayList<DocData>();

            if (keywords.contains(new String(term))) {
                String documentData = parts[1];
                String [] documents = documentData.split("/");
                for (int i = 0; i < documents.length; i++) {
                    if (!documents[i].equals("")) {
                        String [] data = documents[i].split(",");
                        String docName = data[0];
                        int count = 0;
                        for (int j = 1; j < data.length; j++) {
                            if (!data[j].equals("")) {
                                count += Integer.parseInt(data[j].split(":")[1]);
                            }
                        }
                        counts.add(new DocData(docName, count));
                    }
                }

                DocData [] docList = counts.toArray(new DocData[counts.size()]);
                Arrays.sort(docList);
                for (int i = 0; i < docList.length; i++) {
                    resultWriter.println(docList[i].docName);
                }
            }
        }

        private class DocData implements Comparable<DocData> {
            String docName;
            int count;

            public DocData(String name, int c) {
                this.docName = new String(name);
                this.count = c;
            }

            public int compareTo(DocData d) {
                if (this.count < d.count) return 1;
                else if (this.count > d.count) return -1;
                else return 0;
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
