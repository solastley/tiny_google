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
    final static int NUMBER_OF_CHUNKS = 30847;
    final static int CHUNK_SIZE = 512;

    private static List<String> keywords;
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
        keywords = lem.lemmatize(query);

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
        //maps keywords to how many chunks contained the keyword
        private HashMap<String, Integer> chunkFreq = new HashMap<>();
        //maps docName/chunk to corresponding DocData object
        HashMap<String, DocData> counts = new HashMap<>();


        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String [] parts = line.split("\t");
            String term = parts[0];
            
            //if current line is a keyword
            if (keywords.contains(new String(term))) {
                String documentData = parts[1];
                String [] documents = documentData.split("/");
                int chunkCount = 0;
                //iterate over books
                for (int i = 0; i < documents.length; i++) {
                    if (!documents[i].equals("")) {
                        String [] data = documents[i].split(",");
                        String docName = data[0];
                        int count = 0;
                        int chunk = -1;
                        chunkCount += data.length;
                        //iterate over chunks in book
                        for (int j = 1; j < data.length; j++) {
                            if (!data[j].equals("")) {
                                String [] chunkData = data[j].split(":");
                                chunk = Integer.parseInt(chunkData[0]); 
                                count = Integer.parseInt(chunkData[1]);
                                insertChunkData(docName, chunk, term, count);
                            }
                        }
                    }
                }

                if(!chunkFreq.containsKey(term)) 
                    chunkFreq.put(term, chunkCount);
                else 
                    chunkFreq.put(term, chunkFreq.get(term)+chunkCount);

                DocData [] docList = new DocData[counts.size()];
                Iterator docDataIt = counts.entrySet().iterator();
                int index = 0;
                while(docDataIt.hasNext()){
                    Map.Entry pair = (Map.Entry) docDataIt.next();
                    docList[index] = (DocData) pair.getValue();
                    index++;
                }

                Arrays.sort(docList);
                for (int i = 0; i < 5; i++) {
                    printChunk(docList[i].docName, docList[i].chunk);
                }
            }
        }

        private void insertChunkData(String book, int chunk, String term, int count){
            if(!counts.containsKey(book + "/" + String.valueOf(chunk)))
                counts.put(book + "/" + String.valueOf(chunk), new DocData(book, chunk));

            DocData d = counts.get(book + "/" + String.valueOf(chunk));
            d.termFreq.put(term,count);
        }

        private void printChunk(String book, int chunk) throws FileNotFoundException, IOException{
            File doc = new File("/tmp/tiny_google_input/"+book);
            byte[] context = new byte[512];
            RandomAccessFile file = new RandomAccessFile(doc, "r");
            file.seek(chunk*CHUNK_SIZE);
            file.readFully(context);
            resultWriter.println(book + " " + chunk);
            resultWriter.println(new String(context));
        }

        private class DocData implements Comparable<DocData> {
            String docName;
            int chunk;
            //stores frequencies of keywords
            HashMap<String, Integer> termFreq;
            float weight = -1;

            public DocData(String name, int chunk) {
                this.docName = new String(name);
                this.chunk = chunk;
                this.termFreq = new HashMap<>();
            }

            public float computeWeight(){
                if(weight == -1){
                    Iterator termIt = termFreq.entrySet().iterator();
                    while(termIt.hasNext()){
                        Map.Entry pair = (Map.Entry)termIt.next();
                        String term = (String)pair.getKey();
                        int freq = (int)pair.getValue();
                        int chunks = chunkFreq.get(term);
                        float frac = NUMBER_OF_CHUNKS/chunks;
                        weight = 0;
                        weight += (freq*Math.log(frac));
                    }
                }
                return this.weight;
            }

            public int compareTo(DocData d) {
                if (this.computeWeight() < d.computeWeight()) return 1;
                else if (this.computeWeight() > d.computeWeight()) return -1;
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
            public ArrayList<String> lemmatize(String documentText) {
                ArrayList<String> lemmas = new ArrayList<>();
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
