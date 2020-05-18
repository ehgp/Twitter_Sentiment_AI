package com.example;


import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.Serializable;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.fasterxml.jackson.databind.ObjectMapper;
import backtype.storm.spout.SchemeAsMultiScheme;
import org.json.simple.JSONObject;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import storm.kafka.ZkHosts;



public class StormTwitter
{


    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();

        if (args != null && args.length > 0)
        {
            StormSubmitter.submitTopology(
                    args[0],
                    createConfig(false),
                    createTopology());
        }
        else
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(
                    "StormTwitter",
                    createConfig(true),
                    createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
    private final Logger LOGGER = Logger.getLogger(this.getClass());
    private static StormTopology createTopology()
    {
        SpoutConfig kafkaConf = new SpoutConfig(
                new ZkHosts("localhost:2181"),
                "Twitter-API",
                "/Twitter-API",
                "id");
        kafkaConf.bufferSizeBytes = 1024*1024*4;
        kafkaConf.fetchSizeBytes = 1024*1024*4;
        kafkaConf.forceFromStart = true;
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder topology = new TopologyBuilder();
        topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), 4);
        topology.setBolt("twitter_filter", new TwitterFilterBolt(), 4).shuffleGrouping("kafka_spout");
        topology.setBolt("text_filter", new TextFilterBolt(), 4).shuffleGrouping("twitter_filter");
        topology.setBolt("stemming", new StemmingBolt(), 4).shuffleGrouping("text_filter");
        topology.setBolt("positive", new PositiveSentimentBolt(), 4).shuffleGrouping("stemming");
        topology.setBolt("negative", new NegativeSentimentBolt(), 4).shuffleGrouping("stemming");
        topology.setBolt("join", new JoinSentimentsBolt(), 4).fieldsGrouping("positive", new Fields("tweet_id")).fieldsGrouping("negative", new Fields("tweet_id"));
        topology.setBolt("score", new SentimentScoringBolt(), 4).shuffleGrouping("join");
        topology.setBolt("hdfs", new HDFSBolt(), 4).shuffleGrouping("score");

        return topology.createTopology();
    }

    private static Config createConfig(boolean local)
    {
        int workers = 4;
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }
}


class TwitterFilterBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
            Logger.getLogger(TwitterFilterBolt.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id", "tweet_text"));
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        /*LOGGER.debug("filtering incoming tweets");*/
        String json = input.getString(0);
        json = json.replaceAll("StatusJSONImpl", "");
        json = json.replaceAll("\n", "");
        json = json.replaceAll("'", "");
        json = json.replaceAll("(, source)=(?s)(.*$)", ",}");
        json = json.replaceAll("\"", "");
        json = json.replace("\\", "");
        json = json.replaceAll("(createdAt)=(.+?),", "\"$1\":\"$2\",");
        json = json.replaceAll("(id)=(.+?),", "\"$1\":\"$2\",");
        json = json.replaceAll("(text)=(.+)+,", "\"$1\":\"$2\",");
        json = json.replaceAll(",}", ", \"lang\":\"en\"}");
        json = json.replaceAll(", \"userMentionEntities\":\"\\[]\",", "");
       try
        {
            JSONObject root = mapper.readValue(json, JSONObject.class);
            Long id;
            String text;
            if (root.get("lang") != null &&
                    "en".equals(root.get("lang")))
            {
                if (root.get("id") != null && root.get("text") != null && root != null)
                {
                    id = Long.parseLong((String) root.get("id"));
                    text = (String) root.get("text");
                    collector.emit(new Values(id, text));
                }

                else
                    LOGGER.debug("tweet id and/ or text was null");
            }
            else
                LOGGER.debug("Ignoring non-english tweet");
        }
        catch (IOException ex)
        {
            LOGGER.error("IO error while filtering tweets", ex);
            LOGGER.trace(null, ex);
        }
    }

    public Map<String, Object> getComponentConfiguration() { return null; }
}



class TextFilterBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static Logger LOGGER = Logger.getLogger(TextFilterBolt.class);

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id", "tweet_text"));
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        /*LOGGER.debug("removing ugly characters");*/
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String text = input.getString(input.fieldIndex("tweet_text"));
        text = text.replaceAll("[^a-zA-Z\\s]", " ").trim().toLowerCase();
        collector.emit(new Values(id, text));
    }

    public Map<String, Object> getComponentConfiguration() { return null; }
}


class StemmingBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static Logger LOGGER = Logger.getLogger(StemmingBolt.class);

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id", "tweet_text"));
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        /*LOGGER.debug("removing stop words");*/
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String text = input.getString(input.fieldIndex("tweet_text"));
        List<String> allWords = new ArrayList<>(Arrays.asList(text.toLowerCase().split(" ")));
        List<String> stopWords = StopWords.getStopWords("C:/Users/user/Desktop/stopwords.txt");
        allWords.removeAll(stopWords);
        String result = String.join(" ", allWords);
        collector.emit(new Values(id, result));

    }

    public Map<String, Object> getComponentConfiguration() { return null; }
}

class StopWords
{
    public static List<String> getStopWords(String fileName)
    {
        List<String> lines = Collections.emptyList();
        try
        {
            lines =
                    Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
        }
        catch (IOException e)
        {
            // do something
            e.printStackTrace();
        }
        return lines;
    }
}

class SentimentScoringBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
            Logger.getLogger(SentimentScoringBolt.class);

    public void execute(Tuple tuple, BasicOutputCollector collector)
    {
        /*LOGGER.debug("Scoring tweet");*/
        Long id = tuple.getLong(tuple.fieldIndex("tweet_id"));
        String text = tuple.getString(tuple.fieldIndex("tweet_text"));
        Float pos = tuple.getFloat(tuple.fieldIndex("pos_score"));
        Float neg = tuple.getFloat(tuple.fieldIndex("neg_score"));
        String score = pos > neg ? "positive" : "negative";
        /*LOGGER.debug(String.format("tweet %s: %s", id, score));*/
        collector.emit(new Values(id, text, pos, neg, score));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(
                "tweet_id",
                "tweet_text",
                "pos_score",
                "neg_score",
                "score"));
    }
}



class PositiveSentimentBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
            Logger.getLogger(PositiveSentimentBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        /*LOGGER.debug("Calculating positive score");*/
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String text = input.getString(input.fieldIndex("tweet_text"));
        List<String> posWords = PositiveWords.getPosWords("C:/Users/user/Desktop/poswords.txt");
        String[] words = text.split(" ");
        int numWords = words.length;
        int numPosWords = 0;
        for (String word : words)
        {
            if (posWords.contains(word))
                numPosWords++;
        }
        collector.emit(new Values(id, (float) numPosWords / numWords, text));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id", "pos_score", "tweet_text"));
    }
}

class PositiveWords
{
    public static List<String> getPosWords(String fileName)
    {
        List<String> lines = Collections.emptyList();
        try
        {
            lines =
                    Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
        }
        catch (IOException e)
        {
            // do something
            e.printStackTrace();
        }
        return lines;
    }
}



class NegativeSentimentBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
            Logger.getLogger(NegativeSentimentBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        /*LOGGER.debug("Calculating negative score");*/
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String text = input.getString(input.fieldIndex("tweet_text"));
        List<String> negWords = NegativeWords.getNegWords("C:/Users/user/Desktop/negwords.txt");
        String[] words = text.split(" ");
        int numWords = words.length;
        int numNegWords = 0;
        for (String word : words)
        {
            if (negWords.contains(word))
                numNegWords++;
        }
        collector.emit(new Values(id, (float)numNegWords / numWords, text));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id", "neg_score", "tweet_text"));
    }
}


    class NegativeWords
    {
        public static List<String> getNegWords(String fileName)
        {
            List<String> lines = Collections.emptyList();
            try
            {
                lines =
                        Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                // do something
                e.printStackTrace();
            }
            return lines;
        }
    }



class JoinSentimentsBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
            Logger.getLogger(JoinSentimentsBolt.class);
    private HashMap<Long, Triple<String, Float, String>> tweets;

    public JoinSentimentsBolt()
    {
        this.tweets = new HashMap<Long, Triple<String, Float, String>>();
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String text = input.getString(input.fieldIndex("tweet_text"));
        if (input.contains("pos_score"))
        {
            Float pos = input.getFloat(input.fieldIndex("pos_score"));
            if (this.tweets.containsKey(id))
            {
                Triple<String, Float, String> triple = this.tweets.get(id);
                if ("neg".equals(triple.getCar()))
                    emit(collector, id, triple.getCaar(), pos, triple.getCdr());
                else
                {
                    LOGGER.warn("one sided join attempted");
                    this.tweets.remove(id);
                }
            }
            else
                this.tweets.put(
                        id,
                        new Triple<String, Float, String>("pos", pos, text));
        }
        else if (input.contains("neg_score"))
        {
            Float neg = input.getFloat(input.fieldIndex("neg_score"));
            if (this.tweets.containsKey(id))
            {
                Triple<String, Float, String> triple = this.tweets.get(id);
                if ("pos".equals(triple.getCar()))
                    emit(collector, id, triple.getCaar(), neg, triple.getCdr());
                else
                {
                    LOGGER.warn("one sided join attempted");
                    this.tweets.remove(id);
                }
            }
            else
                this.tweets.put(
                        id,
                        new Triple<String, Float, String>("neg", neg, text));
        }
        else
            throw new RuntimeException("wat");
    }

    private void emit(
            BasicOutputCollector collector,
            Long id,
            String text,
            float pos,
            float neg)
    {
        collector.emit(new Values(id, pos, neg, text));
        this.tweets.remove(id);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id",
                "pos_score",
                "neg_score",
                "tweet_text"));
    }
}


class Triple<T1 extends Serializable,
        T2 extends Serializable,
        T3 extends Serializable> implements Serializable
{
    private static final long serialVersionUID = 42L;

    private T1 car;
    private T2 cdr;
    private T3 caar;

    public Triple(T1 car, T2 cdr, T3 caar)
    {
        this.setCar(car);
        this.setCdr(cdr);
        this.setCaar(caar);
    }

    public T1 getCar()
    {
        return this.car;
    }

    public T2 getCdr()
    {
        return this.cdr;
    }

    public T3 getCaar()
    {
        return this.caar;
    }

    public void setCar(T1 value)
    {
        this.car = value;
    }

    public void setCdr(T2 value)
    {
        this.cdr = value;
    }

    public void setCaar(T3 value)
    {
        this.caar = value;
    }
}


class HDFSBolt extends BaseRichBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER = Logger.getLogger(HDFSBolt.class);
    private OutputCollector collector;
    private int id;
    private List<String> tweet_scores;

    @SuppressWarnings("rawtypes")
    public void prepare(
            Map stormConf,
            TopologyContext context,
            OutputCollector collector)
    {
        this.id = context.getThisTaskId();
        this.collector = collector;
        this.tweet_scores = new ArrayList<String>(100);
    }

    public void execute(Tuple input)
    {
        LOGGER.debug("Writing to HDFS after scoring");
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String tweet = input.getString(input.fieldIndex("tweet_text"));
        Float pos = input.getFloat(input.fieldIndex("pos_score"));
        Float neg = input.getFloat(input.fieldIndex("neg_score"));
        String score = input.getString(input.fieldIndex("score"));
        String tweet_score =
                String.format("%s,%s,%f,%f,%s\n", id, tweet, pos, neg, score);
        this.tweet_scores.add(tweet_score);
        if (this.tweet_scores.size() >= 100)
        {
            writeToHDFS();
            this.tweet_scores = new ArrayList<String>(100);
        }
    }

    private void writeToHDFS()
    {
        FileSystem hdfs = null;
        Path file = null;
        OutputStream os = null;
        BufferedWriter wd = null;

        try
        {
            Configuration conf = new Configuration();
            conf.addResource(new Path("C:/Spark/spark-3.0.0-preview2-bin-hadoop2.7/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("C:/Spark/spark-3.0.0-preview2-bin-hadoop2.7/etc/hadoop/hdfs-site.xml"));
            hdfs = FileSystem.get(conf);
            file = new Path(
                    "hdfs://0.0.0.0:19000/"+this.id+".csv");
            if (hdfs.exists(file))
                os = hdfs.append(file);
            else
                os = hdfs.create(file);
            wd = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            for (String tweet_score : tweet_scores)
            {
                wd.write(tweet_score);
            }
        }
        catch (IOException ex)
        {
            LOGGER.error("Failed to write tweet score to HDFS", ex);
            LOGGER.trace(null, ex);
        }
        finally
        {
            try
            {
                if (os != null) os.close();
                if (wd != null) wd.close();
                if (hdfs != null) hdfs.close();
            }
            catch (IOException ex)
            {
                LOGGER.fatal("IO Exception thrown while closing HDFS"+ " " +this.id, ex);
                LOGGER.trace(null, ex);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}