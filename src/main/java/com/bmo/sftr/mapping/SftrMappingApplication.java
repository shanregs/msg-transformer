package com.bmo.sftr.mapping;

import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({SftrConstants.class, RefDataClient.Properties.class})
public class SftrMappingApplication {
    public static void main(String[] args) {
        SpringApplication.run(SftrMappingApplication.class, args);
    }
}
