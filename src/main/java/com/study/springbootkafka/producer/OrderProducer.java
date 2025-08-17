package com.study.springbootkafka.producer;

import com.study.springbootkafka.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    public static final String TOPIC = "orders";

    public void sendOrder(OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.getOrderId(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message: {}", order.getOrderId(), ex);
                    } else {
                        log.info("Message sent successfully: {}, partition: {}",
                                order.getOrderId(), result.getRecordMetadata().partition());
                    }
                });
    }

    // 비동기 사용하지 않고 동기적으로 메시지를 전송하는 메소드
    public void sendOrderSync(OrderEvent order) throws Exception {
        try {
            SendResult<String, OrderEvent> result = kafkaTemplate.send(TOPIC, order.getOrderId(), order).get();
            log.info("Message sent successfully: {}, partition: {}",
                    order.getOrderId(), result.getRecordMetadata().partition());
        } catch (Exception e) {
            log.error("Error sending message: synchronously", e);
            throw e;
        }
    }
}
