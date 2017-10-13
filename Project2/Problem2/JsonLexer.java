package org.apache.hadoop.examples;

public class JsonLexer {
    private JsonLexerState state;

    public JsonLexer() {
        this(JsonLexerState.NULL);
    }

    public JsonLexer(JsonLexerState initState) {
        state = initState;
    }

    public JsonLexerState getState() {
        return state;
    }

    public void setState(JsonLexerState state) {
        this.state = state;
    }

    public static enum JsonLexerState {
        NULL,
        DONT_CARE,
        BEGIN_OBJECT,
        END_OBJECT,
        QUOTATION,
        WHITESPACE
    }

    public void lex(char c) {
        switch (state) {
            case NULL:
            case BEGIN_OBJECT:
            case END_OBJECT:
            case QUOTATION:
            case DONT_CARE:
            case WHITESPACE: {
                if (Character.isWhitespace(c)) {
                    state = JsonLexerState.WHITESPACE;
                    break;
                }
                switch (c) {

                    case '"':
                        state = JsonLexerState.QUOTATION;
                        break;

                    case '{':
                        state = JsonLexerState.BEGIN_OBJECT;
                        break;

                    case '}':
                        state = JsonLexerState.END_OBJECT;
                        break;
                    default:
                        state = JsonLexerState.DONT_CARE;
                }
                break;
            }
        }
    }
}
