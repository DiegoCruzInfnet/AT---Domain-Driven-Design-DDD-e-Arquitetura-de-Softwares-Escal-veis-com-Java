package br.com.freela.notificacao;

class EventoInvalidoException extends RuntimeException {
    EventoInvalidoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}