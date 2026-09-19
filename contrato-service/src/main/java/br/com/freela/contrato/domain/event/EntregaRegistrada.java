package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

public record EntregaRegistrada(UUID eventId, Instant occurredAt, UUID contratoId,
        UUID clienteId, UUID freelancerId)
        implements DomainEvent {
    public static EntregaRegistrada novo(Contrato c) {
        return new EntregaRegistrada(UUID.randomUUID(), Instant.now(), c.id(), c.clienteId(), c.freelancerId());
    }

    @Override
    @JsonProperty("eventType")
    public String eventType() {
        return "EntregaRegistrada";
    }
}
