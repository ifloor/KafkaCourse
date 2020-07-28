package br.com.monitoratec.consolidador.kafka.listeners;

import kafka.avro.generated.Selling;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EmissorListener {

    double totalSelling = 0;

    @KafkaListener(topics = "br.com.monitoratec.selling")
    public void listen(Selling selling) {
        System.out.println("New selling, amount: [U$S " + selling.getAmount() + "]");

        totalSelling += selling.getAmount();
        System.out.println("Total daily sells: [" + totalSelling + "]\n");
    }
}
