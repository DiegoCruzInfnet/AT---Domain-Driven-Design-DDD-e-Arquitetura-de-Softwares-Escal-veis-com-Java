package br.com.freela.notificacao;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@Entity
@Table(name = "notificacoes")
class Notificacao {
    @Id
    UUID id;
    UUID contratoId;
    UUID destinatarioId;
    String tipo;
    @Column(length = 500)
    String mensagem;
    Instant criadaEm;

    protected Notificacao() {
    }

    Notificacao(UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {
        this.id = UUID.randomUUID();
        this.contratoId = contratoId;
        this.destinatarioId = destinatarioId;
        this.tipo = tipo;
        this.mensagem = mensagem;
        this.criadaEm = Instant.now();
    }
}
