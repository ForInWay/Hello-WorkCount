package com.morgan.workcount.component;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 单词计算blot
 */
public class WordCount extends BaseRichBolt {

    private static final long serialVersionUID = 7208077706057284643L;

    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    private OutputCollector collector;
    private Map<String,Long> wordCounts = new HashMap<String,Long>();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");

        Long count = wordCounts.get(word);
        if (count == null){
            count = 0L;
        }
        count++;
        wordCounts.put(word,count);
        logger.info("单词[" + word + "],出现次数" + count);
        collector.emit(new Values(word,count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }
}
