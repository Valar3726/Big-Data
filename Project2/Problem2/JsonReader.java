package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.InputStream;


import org.apache.hadoop.examples.JsonLexer.JsonLexerState;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;



public class JsonReader {

    private final InputStream is;
    private final InputStreamReader inputStreamReader;
    private final JsonLexer lexer;
    private long bytesRead = 0;
    private boolean endOfStream;

    public JsonReader(InputStream is) {
        this.is = is;
        this.lexer = new JsonLexer();


        this.inputStreamReader = new InputStreamReader(is, StandardCharsets.UTF_8);
    }

    private boolean scanToFirstBeginObject() throws IOException {

        char prev = ' ';
        int i;
        while ((i = inputStreamReader.read()) != -1) {
            char c = (char) i;
            bytesRead++;
            if (c == '{' && prev != '\\') {
                lexer.setState(JsonLexer.JsonLexerState.BEGIN_OBJECT);
                return true;
            }
            prev = c;
        }
        endOfStream = true;
        return false;
    }


    public String nextObject() throws IOException {

        if (endOfStream) {
            return null;
        }
        int i;

        StringBuilder currentObject = new StringBuilder();


        if (!scanToFirstBeginObject()) {
            return null;
        }


        while ((i = inputStreamReader.read()) != -1) {
            char c = (char) i;
            bytesRead++;

            lexer.lex(c);
            if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
                currentObject.setLength(0);
            } else if(lexer.getState() == JsonLexerState.QUOTATION || lexer.getState() == JsonLexerState.WHITESPACE) {
            	continue;
            } else if(lexer.getState() == JsonLexerState.DONT_CARE) {
            	currentObject.append(c);
            } else if(lexer.getState() == JsonLexerState.END_OBJECT) {
            	return currentObject.toString();
            } else {
            	break;
            }
        }
        endOfStream = true;
        return null;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public boolean isEndOfStream() {
        return endOfStream;
    }
}
