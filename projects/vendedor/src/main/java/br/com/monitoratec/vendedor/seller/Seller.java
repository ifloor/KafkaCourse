package br.com.monitoratec.vendedor.seller;

import br.com.monitoratec.vendedor.kafka.avro.generated.Selling;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

@Component
public class Seller {

    @Autowired
    KafkaTemplate<String, Selling> sellingKafkaTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void doSomethingAfterStartup() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Scanner scanner = new Scanner(System.in);

        String userInput = "";
        while(!userInput.equals("Goodbye")) {
            System.out.println("To do a sell, let the data in the pattern: \n<buyer name>, amount");
            System.out.println("If you want to quit, type: Goodbye");

            userInput = scanner.nextLine();

            if (!userInput.equals("Goodbye")) {
                this.sell(userInput);
            }
        }

    }

    private void sell(String userInput) {
        if (userInput == null) {
            System.out.println("Impossible to understand, please follow the pattern");
            return;
        }

        String[] pieces = userInput.split(",");
        if (pieces.length != 2) {
            System.out.println("Impossible to understand, please follow the pattern");
            return;
        }

        String buyer = pieces[0];
        double amount = 0;
        try {
            amount = Double.parseDouble(pieces[1]);
        } catch (Exception e) {
            System.out.println("Impossible to parse the amount, please follow the pattern: 00.00");
            return;
        }

        Selling selling = new Selling();
        selling.setBuyer(buyer);
        selling.setAmount(amount);

        try {
            sellingKafkaTemplate.sendDefault(selling).completable().get();
            System.out.println("Wrote data to broker =]");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
