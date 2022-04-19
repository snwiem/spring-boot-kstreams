package org.syracus.kstream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

@SpringBootApplication
@Slf4j
public class KstreamAppApplication {

    public static void main(String[] args) {
        SpringApplication.run(KstreamAppApplication.class, args);
    }


    @Bean
    public Consumer<KStream<String, String>> handleEvent(SomeService service) {
        log.debug("###### CREATING CONSUMER ######");
        return input -> input
                .foreach((k, v) -> {
                    log.debug("********************************************\n********************************************\n********************************************\n********************************************");
                    service.doSomethingImportantWithData(v);
                });
    }
}
