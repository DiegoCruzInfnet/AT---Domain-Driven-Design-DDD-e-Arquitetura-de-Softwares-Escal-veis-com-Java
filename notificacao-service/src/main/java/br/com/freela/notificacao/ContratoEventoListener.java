package br.com.freela.notificacao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
public class ContratoEventoListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventoListener.class);

    private final NotificacaoService notificacaoService;
    private final EventoProcessadoRepository eventoProcessadoRepository;
    private final ObjectMapper objectMapper;

    public ContratoEventoListener(NotificacaoService notificacaoService,
            EventoProcessadoRepository eventoProcessadoRepository,
            ObjectMapper objectMapper) {
        this.notificacaoService = notificacaoService;
        this.eventoProcessadoRepository = eventoProcessadoRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "contrato-eventos", groupId = "notificacao-service")
    @Transactional
    public void ouvir(String mensagem) {
        JsonNode node;
        try {
            node = objectMapper.readTree(mensagem);
        } catch (Exception e) {
            throw new EventoInvalidoException("Falha ao parsear evento: " + e.getMessage(), e);
        }

        String correlationId = node.path("correlationId").asText(null);
        MDC.put("correlationId", correlationId != null ? correlationId : "sem-correlation-id");

        try {
            UUID eventId = UUID.fromString(node.path("eventId").asText());

            if (eventoProcessadoRepository.existsById(eventId)) {
                log.info("notificacao.evento.duplicado.ignorado eventId={}", eventId);
                return;
            }

            String eventType = node.path("eventType").asText();
            UUID contratoId = UUID.fromString(node.path("contratoId").asText());
            UUID clienteId = UUID.fromString(node.path("clienteId").asText());
            UUID freelancerId = UUID.fromString(node.path("freelancerId").asText());

            log.info("notificacao.evento.recebido eventId={} contratoId={} eventType={}", eventId, contratoId,
                    eventType);

            String texto = montarMensagem(eventType);
            notificacaoService.registrar(contratoId, clienteId, eventType, texto);
            notificacaoService.registrar(contratoId, freelancerId, eventType, texto);

            eventoProcessadoRepository.save(new EventoProcessado(eventId));

            log.info("notificacao.evento.processado eventId={} contratoId={}", eventId, contratoId);
        } finally {
            MDC.remove("correlationId");
        }
    }

    private String montarMensagem(String eventType) {
        return switch (eventType) {
            case "ContratoCriado" -> "Um novo contrato foi criado.";
            case "EntregaRegistrada" -> "A entrega do contrato foi registrada.";
            case "ContratoConcluido" -> "O contrato foi concluído.";
            case "ContratoCancelado" -> "O contrato foi cancelado.";
            default -> "Atualização no contrato: " + eventType;
        };
    }
}