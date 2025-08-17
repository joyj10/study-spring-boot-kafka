package com.study.springbootkafka.producer;

import org.junit.jupiter.api.DisplayName;
import com.study.springbootkafka.model.OrderEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"orders"}, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class OrderProducerTest {
    @Autowired
    private OrderProducer producer;

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    private Consumer<String, OrderEvent> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "localhost:9092",
                "test-group",
                "true"
        );

        JsonDeserializer<OrderEvent> deserializer = new JsonDeserializer<>(OrderEvent.class, false);
        deserializer.addTrustedPackages("*");

        consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                deserializer
        ).createConsumer();

        consumer.subscribe(List.of("orders"));  // 테스트용 토픽
        consumer.poll(Duration.ofMillis(100));      // 초기 데이터를 읽지 않도록 빠르게 폴링
        consumer.seekToBeginning(consumer.assignment()); // 오프셋 초기화
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @DisplayName("주문 메시지 Kafka 전송")
    void testSendOrder() {
        // given
        OrderEvent order = createTestOrder();

        // when
        producer.sendOrder(order);

        // then
        ConsumerRecord<String, OrderEvent> record = KafkaTestUtils.getSingleRecord(consumer, "orders");
        assertThat(record).isNotNull();
        assertThat(record.value().getOrderId()).isEqualTo(order.getOrderId());
    }

    private OrderEvent createTestOrder() {
        List<OrderEvent.OrderItem> items = List.of(new OrderEvent.OrderItem("prod-1", 2, BigDecimal.valueOf(20.00)));
        return new OrderEvent("order-123", "cust-456", items, BigDecimal.valueOf(40.00), LocalDateTime.now());
    }
}