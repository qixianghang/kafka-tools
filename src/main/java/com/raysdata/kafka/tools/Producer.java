package com.raysdata.kafka.tools;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;

/**
 * kafka producer
 * 
 * @author QXH
 *
 */
public class Producer {
	private Properties props = new Properties();
	private KafkaProducer<String, String> producer;
	
	private String clientId = "producer";//kafka生产实例名称
	//private String certPath = "C:/Users/QXH/Desktop/ssl/client.truststore.jks";
	private String certPath = "/opt/cyancloud/component/proxy/client.truststore.jks";//证书库路径
	private String password = "123456";//证书密码
	private boolean SSL = false;//是否启用SSL
	private String bootstarpServices;//kafka地址和端口，例如：10.10.10.1:9092,10.10.10.2:9092
	private String topic;//kafka topic
	
	/**
	 * 
	 * 向kafka发送数据
	 * 
	 * @param bootstarpServices
	 * 		kafka地址和端口，例如：10.10.10.1:9092,10.10.10.2:9092
	 * @param topic
	 * 		kafka topic
	 * @param args
	 * 		动态参数：[0]kafka clientId，数据类型为String，可选，默认为producer;
	 * 				 [1]证书密码，数据类型为String，可选，如需修改默认密码请携带此参数;
	 * 				
	 */
	public Producer(String bootstarpServices, String topic, String... args){
		if (null != args && args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (i == 0) {
					this.clientId = args[i];
				}
				
				if (i == 1) {
					if (args[i] instanceof String) {
					this.password = args[i];
					}
				}
			}
		}
		this.bootstarpServices = bootstarpServices;
		this.topic = topic;
		initConfig();
	}
	
	/**
	 * 
	 * 向kafka发送数据
	 * 
	 * @param bootstarpServices
	 * 		kafka地址和端口，例如：10.10.10.1:9092,10.10.10.2:9092
	 * @param topic
	 * 		kafka topic
	 * @param SSL
	 * 		是否启用SSL，默认为false不启用；false：不启用；true：启用
	 * @param args
	 * 		动态参数：[0]kafka clientId，数据类型为String，可选，默认为producer;
	 * 				 [1]证书密码，数据类型为String，可选，如需修改默认密码请携带此参数;
	 * 				
	 */
	public Producer(String bootstarpServices, String topic, boolean SSL, String... args){
		if (null != args && args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (i == 0) {
					this.clientId = args[i];
				}
				
				if (i == 1) {
					if (args[i] instanceof String) {
					this.password = args[i];
					}
				}
			}
		}
		this.bootstarpServices = bootstarpServices;
		this.topic = topic;
		this.SSL = SSL;
		initConfig();
	}
	
	private void initConfig(){
		if (SSL) {
			props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
			props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, certPath);  
			props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
		}

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");  
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpServices);  
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		producer = new KafkaProducer<String, String>(props);
	}

	/**
	 * 向kafka发送数据方法，发送后请flush或close
	 * @param record
	 * 		消息体
	 */
	public void send(String record) {
		producer.send(new ProducerRecord<String, String>(this.topic, record));
	}
	
	public void flush(){
		producer.flush();
	}

	public void close() {
		producer.close();
	}
}
