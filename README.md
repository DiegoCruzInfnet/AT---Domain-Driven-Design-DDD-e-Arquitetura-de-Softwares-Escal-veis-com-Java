# Freela Marketplace — Assessment de Comunicação Orientada a Eventos

Evolução do sistema **Freela Marketplace** para um modelo de comunicação assíncrona orientada a eventos entre microsserviços, usando Apache Kafka — com publicação transacional, idempotência, tratamento de falhas, rastreamento distribuído e logs centralizados.


## Arquitetura 

O `contrato-service` publica eventos de domínio (`ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido`, `ContratoCancelado`) no tópico Kafka `contrato-eventos`, usando o **Transactional Outbox Pattern** para garantir consistência entre o banco de dados e a publicação. Três serviços auxiliares — `notificacao-service`, `reputacao-service` e `auditoria-service` — consomem esses eventos de forma independente e idempotente, sem nenhuma chamada HTTP síncrona entre eles. Todo o fluxo é rastreável de ponta a ponta via `correlationId` e Zipkin, com logs centralizados no Elasticsearch/Kibana.

## Stack

Java 21 · Spring Boot 4.1 · Spring Cloud (Gateway, Eureka, LoadBalancer) · Apache Kafka (KRaft) · PostgreSQL · Micrometer Tracing + Zipkin · Filebeat + Elasticsearch + Kibana · Docker Compose

## Serviços

| Serviço | Porta | Descrição |
|---|---|---|
| `eureka-server` | 8761 | Service discovery |
| `api-gateway` | 8080 | Ponto de entrada HTTP, roteamento, geração de `correlationId` |
| `contrato-service` | 8081 | Domínio do contrato; produtor de eventos |
| `notificacao-service` | 8082 | Consumidor — registra notificações |
| `reputacao-service` | 8083 | Consumidor — agrega reputação de freelancers |
| `auditoria-service` | 8084 | Consumidor — arquiva todos os eventos |

## Como rodar

Pré-requisitos: **Docker Desktop**, **Java 21**, **Maven**.

```powershell
# 1. Infraestrutura (Postgres, Kafka, Zipkin, Elasticsearch, Kibana, Filebeat)
cd infra
docker compose up -d
docker compose ps

# 2. Microsserviços (cada um em um terminal, a partir da raiz do projeto)
cd ..
mvn -pl eureka-server spring-boot:run
mvn -pl contrato-service spring-boot:run
mvn -pl notificacao-service spring-boot:run
mvn -pl auditoria-service spring-boot:run
mvn -pl reputacao-service spring-boot:run
mvn -pl api-gateway spring-boot:run
```

## Testando 

```powershell
# Cria um contrato (via Gateway, porta 8080)
curl.exe -X POST http://localhost:8080/api/contratos `
  -H "Content-Type: application/json" `
  -d '{"clienteId":"11111111-1111-1111-1111-111111111111","freelancerId":"22222222-2222-2222-2222-222222222222","titulo":"Teste","valor":100.00}'

# Avança o ciclo de vida (troque {id} pelo id retornado acima)
curl.exe -X POST http://localhost:8080/api/contratos/{id}/entrega
curl.exe -X POST http://localhost:8080/api/contratos/{id}/conclusao
```

## Ferramentas de observabilidade

| Ferramenta | URL |
|---|---|
| Eureka Dashboard | http://localhost:8761 |
| Kafka UI | http://localhost:8090 |
| Zipkin (tracing) | http://localhost:9411 |
| Kibana (logs) | http://localhost:5601 |

