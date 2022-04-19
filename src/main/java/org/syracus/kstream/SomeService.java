package org.syracus.kstream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SomeService {

    public void doSomethingImportantWithData(String data) {
        log.info("****** I RECEIVED '{}' ******", data);
    }
}
