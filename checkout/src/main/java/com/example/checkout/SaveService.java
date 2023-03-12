package com.example.checkout;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Slf4j
@RequiredArgsConstructor
public class SaveService {
    private final static String CHECKOUT_COMPLETE_TOPIC_NAME = "checkout.complete.v1";

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ModelMapper modelMapper = new ModelMapper();

    private final CheckOutRepository checkOutRepository;

    public Long saveCheckOutData(CheckoutDto CheckoutDto) {
        CheckoutEntity CheckoutEntity = saveDatabase(CheckoutDto);

        CheckoutDto.setCheckOutId(CheckoutEntity.getCheckOutId());
        CheckoutDto.setCreatedAt(new Date(CheckoutEntity.getCreatedAt().getTime()));
        sendToKafka(CheckoutDto);

        return CheckoutEntity.getCheckOutId();
    }

    private CheckoutEntity saveDatabase(CheckoutDto CheckoutDto) {

        CheckoutEntity CheckoutEntity = modelMapper.map(CheckoutDto, CheckoutEntity.class);

        return checkOutRepository.save(CheckoutEntity);

    }

    private void sendToKafka(CheckoutDto CheckoutDto) {
        try {
            String jsonInString = objectMapper.writeValueAsString(CheckoutDto);
            kafkaTemplate.send(CHECKOUT_COMPLETE_TOPIC_NAME, jsonInString);
            log.info("success sendToKafka");
        } catch (Exception e) {
            log.error("fail to send kafka", e);
        }
    }
}

