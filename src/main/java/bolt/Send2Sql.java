package bolt;

import java.util.Map;

import util.MysqlOpt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Send2Sql extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private boolean flag_xml = true;
	private MysqlOpt mysql;
	private String host_port = "172.28.5.92:3306";
	  
	private String database = "DesktopSetting";
	  
	private String username = "syswin";
	  
	private String password = "syswin";
	  
	private String tableName = "stormTest2";

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
	    this.mysql = new MysqlOpt();
	    
	    if (!this.mysql.connSQL(this.host_port, this.database, this.username, this.password)) {
	      this.flag_xml = false;
	    } else {
	      this.flag_xml = true;
	    }
		
	}

	public void execute(Tuple input) {
	    String str = input.getString(0);
	    
	    if (this.flag_xml == true) {
	      if (!this.mysql.insertSQL(str)) {
	        System.out.println("MysqlBolt-- Erre: can't insert tuple into database!");
	        System.out.println("MysqlBolt-- Error Tuple: " + str);
	        System.out.println("SQL: " + str);
	      }
	    }
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
