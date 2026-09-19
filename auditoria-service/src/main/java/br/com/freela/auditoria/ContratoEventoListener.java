package br.com.freela.auditoria;

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

    private final AuditoriaService auditoriaService;
    private final EventoProcessadoRepository eventoProcessadoRepository;
    private final ObjectMapper objectMapper;

    public ContratoEventoListener(AuditoriaService auditoriaService,
            EventoProcessadoRepository eventoProcessadoRepository,
            ObjectMapper objectMapper) {
        this.auditoriaService = auditoriaService;
        this.eventoProcessadoRepository = eventoProcessadoRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "contrato-eventos", groupId = "auditoria-service")
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
                log.info("auditoria.evento.duplicado.ignorado eventId={}", eventId);
                return;
            }

            UUID contratoId = UUID.fromString(node.path("contratoId").asText());
            String eventType = node.path("eventType").asText();

            log.info("auditoria.evento.recebido eventId={} contratoId={} eventType={}", eventId, contratoId, eventType);

            auditoriaService.registrar(eventId, contratoId, eventType, correlationId, mensagem);
            eventoProcessadoRepository.save(new EventoProcessado(eventId));

            log.info("auditoria.evento.processado eventId={} contratoId={}", eventId, contratoId);
        } finally {
            MDC.remove("correlationId");
        }
    }
}