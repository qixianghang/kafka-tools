package com.raysdata.kafka.tools;

class App {
	
	public static void main(String[] args) {
//		Producer producer = new Producer("10.20.32.62:9092", "test");
//		Producer producer = new Producer("10.20.32.62:9093", "test", true);
//		Producer producer = new Producer("10.20.32.62:9093", "test", true, "test");
		Producer producer = new Producer("10.20.32.62:9093", "test", true, "test", null, "C:/Users/QXH/Desktop/ssl/client.truststore.jks");
		for (int i = 0; i < 10; i++) {
			producer.send("test"+i);
		}
//		producer.flush();
		producer.close();
	}
}
