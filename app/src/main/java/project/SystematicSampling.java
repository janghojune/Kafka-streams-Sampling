package project;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.ProcessorContext;
//import org.apache.kafka.streams.state.KeyValueStore;

public class SystematicSampling implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;
    //private KeyValueStore<String, String> stateStore;
    private int counter;
    private int interval;

    public SystematicSampling(int interval) {
        this.interval = interval;
        this.counter = 0;
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
        counter++;
        if (counter % interval == 0) {
            this.context.forward(record);
        }
    }
}
