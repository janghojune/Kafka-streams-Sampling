package project;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;



public class StreamsFilter {

  private static String APPLICATION_NAME = "application"; // 소비자 그룹 id를 생성
  private static String BOOTSTRAP_SERVERS = "SN01:29092,SN02:29092,SN03:29092,SN04:29092,SN05:29092,SN06:29092,SN07:29092,SN08:29092,SN09:29092"; // 카프카 서버 정보

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());


    // 사용자가 알고리즘을 선택하도록 함
    Scanner scanner = new Scanner(System.in);
    System.out.print("Enter the selecte Sampling algorithm (1. Random 2. Systematic) : ");
    int selecte = scanner.nextInt();

    //데이터를 받아올 토픽 이름 입력
    System.out.print("Enter the topic name to read the data : ");
    String input_topic = scanner.nextLine();

    //샘플링 표본을 저장할 토픽 이름 입력
    System.out.print("Enter the topic name to save the sampling results.");
    String result_topic = scanner.nextLine();

    // 스트림 토폴로지를 정의하기 위한 StreamsBuilder
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> userScore = builder.stream(input_topic);

    if(selecte == 1){
      // interval : 랜덤 샘플링을 위한 윈도우 크기, per : 윈도우에서 샘플링할 비율
      System.out.print("Enter the interval number : ");
      int interval = scanner.nextInt();
      System.out.print("Enter the percent(%) number : ");
      int per = scanner.nextInt();

      // RandomSampling 클래스에 interval, per값을 전달하여 랜덤 샘플링 수행
      userScore.process(() -> new RandomSampling(interval, per)).to(result_topic);
      KafkaStreams streams = new KafkaStreams(builder.build(), props);
      streams.start();
      System.out.println("Run RandomSampling...");
    }
    else if(selecte == 2){
      // interval : 계통 샘플링을 위한 간격 값
      System.out.print("Enter the interval number : ");
      int interval = scanner.nextInt();

      // SystematicSampling 클래스에 interval값을 전달하여 계통 샘플링 수행
      userScore.process(() -> new SystematicSampling(interval)).to(result_topic); 
      KafkaStreams streams = new KafkaStreams(builder.build(), props);
      streams.start();
      System.out.println("Run Systematic Sampling...");
    }

    scanner.close();
    
  }
}