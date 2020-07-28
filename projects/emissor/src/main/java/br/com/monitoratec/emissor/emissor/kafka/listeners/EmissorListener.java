package br.com.monitoratec.emissor.emissor.kafka.listeners;

import br.com.monitoratec.vendedor.kafka.avro.generated.Selling;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EmissorListener {

    @KafkaListener(topics = "br.com.monitoratec.selling")
    public void listen(Selling selling) {
        System.out.println("Received selling, buyer: [" + selling.getBuyer() + "], amount: [U$S " + selling.getAmount() + "]");
        System.out.println("Emitting coupon...");
    }
}
