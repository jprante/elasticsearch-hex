
package org.xbib.elasticsearch.common.xcontent;

import org.xbib.elasticsearch.common.xcontent.json.JsonXContent;

/**
 * The content type of {@link XContent}.
 */
public enum XContentType {

    /**
     * A JSON based content type.
     */
    JSON(0) {
        @Override
        public String restContentType() {
            return "application/json";
        }

        @Override
        public String shortName() {
            return "json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }
    }
    ;

    public static XContentType fromRestContentType(String contentType) {
        if (contentType == null) {
            return null;
        }
        if ("application/json".equals(contentType) || "json".equalsIgnoreCase(contentType)) {
            return JSON;
        }
        return null;
    }

    private int index;

    XContentType(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    public abstract String restContentType();

    public abstract String shortName();

    public abstract XContent xContent();
}
