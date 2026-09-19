package br.com.freela.reputacao;

class EventoInvalidoException extends RuntimeException {
    EventoInvalidoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}