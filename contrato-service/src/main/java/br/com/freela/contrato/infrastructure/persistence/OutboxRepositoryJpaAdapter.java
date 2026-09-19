package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.application.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.UUID;

@Repository
public class OutboxRepositoryJpaAdapter implements OutboxRepository {
    private static final Logger log = LoggerFactory.getLogger(OutboxRepositoryJpaAdapter.class);
    private final SpringDataOutboxRepository jpa;

    public OutboxRepositoryJpaAdapter(SpringDataOutboxRepository jpa) { this.jpa = jpa; }

    @Override
    public void registrar(UUID eventId, UUID aggregateId, String eventType, String payload,
                           String correlationId, Instant occurredAt) {
        var entity = new OutboxEvent(eventId, aggregateId, eventType, payload, correlationId, occurredAt);
        jpa.save(entity);
        log.info("contrato.outbox.registrado contratoId={} eventId={} eventType={}", aggregateId, eventId, eventType);
    }
}