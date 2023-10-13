package project;

import java.util.ArrayList;
import java.util.Random;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.ProcessorContext;
//import org.apache.kafka.streams.state.KeyValueStore;

public class RandomSampling implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;
    //private KeyValueStore<String, String> stateStore;
    private int size;

    // interval : 랜덤 샘플링을 위한 데이터 저장 크기
    // per : interval만큼의 데이터를 받았을 때, per(%)만큼의 비율로 데이터를 추출함
    private int interval;
    private int per;
    private ArrayList<Record<String, String>> bucket = new ArrayList<>();
    private ArrayList<Record<String, String>> randomVal = new ArrayList<>();

    //
    private boolean flag = false;
    private long start_time = System.currentTimeMillis();
    private long last_time = System.currentTimeMillis();
    

    public RandomSampling(int interval, int per) {
        this.interval = interval;
        this.per = per;
        this.size = 0;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        //this.stateStore = context.getStateStore("State_store");
    }

    @Override
    public void close() {
        // Clean up any resources
    }

    @Override
    public void process(Record<String, String> record) {
        if(!flag){
            start_time = System.currentTimeMillis();
            flag = true;
        }

        if(record.value() == "finish"){
            last_time = System.currentTimeMillis();
            System.out.println("processing time : " + (last_time - start_time) + "ms");
        }
        //들어오는 record를 bucket에 저장
        bucket.add(record);
        size++;
        System.out.println(record);
        
        //size가 interval만큼 증가했다면, bucket에 대해서 random sampling 진행
        if (size % interval == 0) {
            size = 0;
            randomVal.clear();
            randomVal = setRandomValue(bucket, per);
            //그 결과를 순회하며 context로 전송함
            for(Record<String, String> element : randomVal){
                this.context.forward(element);
            }
        }
    }

    public ArrayList<Record<String, String>> setRandomValue(ArrayList<Record<String, String>> values, int per) {
        // values에서 per%만큼 추출한 값들을 randomVal에 저장하고 return함
        ArrayList<Record<String, String>> randomVal = new ArrayList<>();
        Random random = new Random();
        int lenth = values.size();
        //백분율 환산 개수
        int count = lenth * per / 100;
        
        for (int i = 0; i < count; i++) {
            // 랜덤한 인덱스 추출
            int randomIndex = random.nextInt(lenth);
            // 해당 인덱스의 값을 추출하여 결과 리스트에 추가, 추출된 값을 리스트에서 제거
            randomVal.add(values.get(randomIndex));
            values.remove(randomIndex);
            lenth--;
        }
        this.bucket.clear();
        return randomVal;
    }
}
