package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.infrastructure.persistence.OutboxEvent;
import br.com.freela.contrato.infrastructure.persistence.OutboxStatus;
import br.com.freela.contrato.infrastructure.persistence.SpringDataOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisherScheduler {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final SpringDataOutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.contrato-eventos:contrato-eventos}")
    private String topic;

    public OutboxPublisherScheduler(SpringDataOutboxRepository outboxRepository,
                                     KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void publicarPendentes() {
        List<OutboxEvent> pendentes = outboxRepository.findByStatusOrderByOccurredAtAsc(OutboxStatus.PENDENTE);
        if (pendentes.isEmpty()) {
            return;
        }
        log.info("outbox.publisher.ciclo.inicio quantidade={}", pendentes.size());
        for (OutboxEvent evento : pendentes) {
            publicar(evento);
        }
    }

    private void publicar(OutboxEvent evento) {
        String key = evento.getAggregateId().toString();
        try {
            kafkaTemplate.send(topic, key, evento.getPayload()).get(5, TimeUnit.SECONDS);
            evento.marcarComoPublicado();
            outboxRepository.save(evento);
            log.info("outbox.publisher.sucesso eventId={} contratoId={} eventType={} topico={}",
                    evento.getId(), evento.getAggregateId(), evento.getEventType(), topic);
        } catch (Exception e) {
            log.error("outbox.publisher.falha eventId={} contratoId={} eventType={} erro={}",
                    evento.getId(), evento.getAggregateId(), evento.getEventType(), e.getMessage());
            // não marca como publicado — permanece PENDENTE para nova tentativa no próximo ciclo
        }
    }
}