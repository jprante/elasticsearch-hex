
package org.xbib.elasticsearch.common.xcontent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

/**
 * A generic abstraction on top of handling content, inspired by JSON and pull parsing.
 */
public interface XContent {

    /**
     * The type this content handles and produces.
     */
    XContentType type();

    /**
     * Creates a new generator using the provided output stream.
     */
    XContentGenerator createGenerator(OutputStream os) throws IOException;

    /**
     * Creates a new generator using the provided writer.
     */
    XContentGenerator createGenerator(Writer writer) throws IOException;

    /**
     * Creates a parser over the provided input stream.
     */
    XContentParser createParser(InputStream is) throws IOException;

    /**
     * Creates a parser over the provided reader.
     */
    XContentParser createParser(Reader reader) throws IOException;

    /**
     * Creates a parser over the provided string content.
     */
    XContentParser createParser(String content) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(byte[] data) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(byte[] data, int offset, int length) throws IOException;

}
