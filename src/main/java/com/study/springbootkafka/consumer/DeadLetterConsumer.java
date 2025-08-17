package com.study.springbootkafka.consumer;

import com.study.springbootkafka.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DeadLetterConsumer {
    @KafkaListener(topics = "orders.DLT", groupId = "dlt-group")
    public void listenDLT(@Payload OrderEvent order, Exception exception) {
        log.error("Received failed order in DLT: {}, error: {}", order.getOrderId(), exception.getMessage());
        // DLT에서 받은 메시지 처리 로직 구현
    }
}
