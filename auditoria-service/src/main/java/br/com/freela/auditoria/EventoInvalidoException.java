package br.com.freela.auditoria;

class EventoInvalidoException extends RuntimeException {
    EventoInvalidoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}