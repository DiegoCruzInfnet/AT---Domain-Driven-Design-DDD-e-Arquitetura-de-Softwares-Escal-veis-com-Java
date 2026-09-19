package br.com.freela.reputacao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class ContratoEventoListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventoListener.class);

    private final ReputacaoService reputacaoService;
    private final EventoProcessadoRepository eventoProcessadoRepository;
    private final ObjectMapper objectMapper;

    public ContratoEventoListener(ReputacaoService reputacaoService,
            EventoProcessadoRepository eventoProcessadoRepository,
            ObjectMapper objectMapper) {
        this.reputacaoService = reputacaoService;
        this.eventoProcessadoRepository = eventoProcessadoRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "contrato-eventos", groupId = "reputacao-service")
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
                log.info("reputacao.evento.duplicado.ignorado eventId={}", eventId);
                return;
            }

            String eventType = node.path("eventType").asText();
            UUID contratoId = UUID.fromString(node.path("contratoId").asText());
            UUID freelancerId = UUID.fromString(node.path("freelancerId").asText());

            log.info("reputacao.evento.recebido eventId={} contratoId={} eventType={}", eventId, contratoId, eventType);

            switch (eventType) {
                case "ContratoConcluido" -> {
                    BigDecimal valor = new BigDecimal(node.path("valor").asText());
                    reputacaoService.registrarContratoConcluido(contratoId, freelancerId, valor);
                }
                case "ContratoCancelado" -> reputacaoService.registrarContratoCancelado(contratoId, freelancerId);
                default -> log.info("reputacao.evento.ignorado eventId={} eventType={} (sem impacto na reputação)",
                        eventId, eventType);
            }

            eventoProcessadoRepository.save(new EventoProcessado(eventId));

            log.info("reputacao.evento.processado eventId={} contratoId={}", eventId, contratoId);
        } finally {
            MDC.remove("correlationId");
        }
    }
}