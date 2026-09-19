package br.com.freela.reputacao;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.util.UUID;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@Entity
@Table(name = "reputacoes")
class ReputacaoFreelancer {
    @Id
    UUID freelancerId;
    int contratosConcluidos;
    BigDecimal valorTotal = BigDecimal.ZERO;
    int contratosCancelados;

    protected ReputacaoFreelancer() {
    }

    ReputacaoFreelancer(UUID id) {
        this.freelancerId = id;
    }

    void registrarContrato(BigDecimal valor) {
        contratosConcluidos++;
        valorTotal = valorTotal.add(valor);
    }

    void registrarCancelamento() {
        contratosCancelados++;
    }
}