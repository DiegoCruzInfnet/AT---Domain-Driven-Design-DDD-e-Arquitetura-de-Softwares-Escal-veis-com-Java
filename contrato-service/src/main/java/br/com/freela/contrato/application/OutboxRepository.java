package br.com.freela.contrato.application;

import java.time.Instant;
import java.util.UUID;

public interface OutboxRepository {
    void registrar(UUID eventId, UUID aggregateId, String eventType, String payload,
                    String correlationId, Instant occurredAt);
}