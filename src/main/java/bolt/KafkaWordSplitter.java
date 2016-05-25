package bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KafkaWordSplitter extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
//	private static final Logger LOG = LoggerFactory.getLogger(KafkaWordSplitter.class);
	private static final Logger LOG = LoggerFactory.getLogger("bolt.KafkaWordSplitter");
	OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		LOG.debug(input.getString(0) + "from debug");
		LOG.info(input.getString(0) + "from info");
		LOG.warn(input.getString(0) + "from warn");
		LOG.error(input.getString(0) + "from err");
//		System.out.println("classsssssss" + KafkaWordSplitter.class);
		this.collector.emit(new Values(input.getString(0)));  
		this.collector.ack(input);	
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
