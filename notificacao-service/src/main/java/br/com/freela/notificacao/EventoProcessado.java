package br.com.freela.notificacao;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados")
class EventoProcessado {
    @Id UUID eventId;
    Instant processadoEm;

    protected EventoProcessado() {}
    EventoProcessado(UUID eventId) {
        this.eventId = eventId;
        this.processadoEm = Instant.now();
    }
}