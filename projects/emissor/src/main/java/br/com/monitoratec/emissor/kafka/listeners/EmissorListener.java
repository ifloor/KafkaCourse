package br.com.monitoratec.emissor.kafka.listeners;

import kafka.avro.generated.Selling;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EmissorListener {

    @KafkaListener(topics = "br.com.monitoratec.selling")
    public void listen(Selling selling) {
        System.out.println("Received selling, buyer: [" + selling.getBuyer() + "], amount: [U$S " + selling.getAmount() + "]");
        System.out.println("Emitting coupon...\n");
    }

    @Bean
    public NewTopic adviceTopic() {
        return new NewTopic("br.com.monitoratec.selling", 3, (short) 1);
    }
}
