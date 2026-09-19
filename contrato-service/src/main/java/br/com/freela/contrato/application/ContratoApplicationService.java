package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.UUID;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

       public ContratoApplicationService(ContratoRepository repository, OutboxRepository outboxRepository,
                                       ObjectMapper objectMapper) {
        this.repository = repository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato salvo = repository.salvar(contrato);

        registrarEventosNoOutbox(contrato);

        log.info("contrato.criacao.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        return aplicarTransicao(id, Contrato::registrarEntrega, "entrega");
    }

    @Transactional
    public Contrato concluir(UUID id) {
        return aplicarTransicao(id, Contrato::concluir, "conclusao");
    }

    @Transactional
    public Contrato cancelar(UUID id) {
        return aplicarTransicao(id, Contrato::cancelar, "cancelamento");
    }

    private Contrato aplicarTransicao(UUID id, java.util.function.Consumer<Contrato> transicao, String operacao) {
        log.info("contrato.{}.inicio contratoId={}", operacao, id);
        Contrato contrato = repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));

        transicao.accept(contrato);

        Contrato salvo = repository.salvar(contrato);
        registrarEventosNoOutbox(contrato);

        log.info("contrato.{}.sucesso contratoId={} status={}", operacao, salvo.id(), salvo.status());
        return salvo;
    }
   
        private void registrarEventosNoOutbox(Contrato contrato) {
        for (DomainEvent event : contrato.pullDomainEvents()) {
            String payload = serializar(event, org.slf4j.MDC.get("correlationId"));
            outboxRepository.registrar(event.eventId(), contrato.id(), event.eventType(), payload,
                    null, event.occurredAt());
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={}",
                    contrato.id(), event.eventId(), event.eventType());
        }
    }

        private String serializar(DomainEvent event, String correlationId) {
        try {
            ObjectNode node = objectMapper.valueToTree(event);
            node.put("correlationId", correlationId);
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar evento " + event.eventType(), e);
        }
    }

        @Transactional(readOnly = true)
        public Contrato buscar(UUID id) {
            log.info("contrato.busca.inicio contratoId={}", id);
            var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
            log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
            return contrato;
        }

        @Transactional(readOnly = true)
        public List<Contrato> listar() {
            log.info("contrato.listagem.inicio");
            var contratos = repository.listar();
            log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
            return contratos;
        }
}
