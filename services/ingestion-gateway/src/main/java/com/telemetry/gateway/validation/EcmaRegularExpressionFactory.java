package com.telemetry.gateway.validation;

import com.networknt.schema.regex.RegularExpression;
import com.networknt.schema.regex.RegularExpressionFactory;
import java.util.regex.Pattern;

/**
 * Schema patterns with the ECMA-262 meaning of {@code $} that JSON Schema requires: the very end of the text.
 * java.util.regex also lets {@code $} match before a final line break, so {@code "checkout-api-07\n"} would pass
 * {@code ^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$} and a newline would reach Kafka, logs and the database (FR-ING-04).
 * An unescaped {@code $} outside {@code [...]} is therefore rewritten to {@code \z}; the Python contract tests do the
 * same (libs/telemetry-contracts/README.md, rule 4). Everything else is java.util.regex as before.
 */
public final class EcmaRegularExpressionFactory implements RegularExpressionFactory {

    public static final EcmaRegularExpressionFactory INSTANCE = new EcmaRegularExpressionFactory();

    private EcmaRegularExpressionFactory() {}

    @Override
    public RegularExpression getRegularExpression(String regex) {
        Pattern pattern = Pattern.compile(ecmaAnchors(regex));
        return value -> pattern.matcher(value).find();
    }

    /** {@code $} (not escaped, not inside a character class) becomes {@code \z}. */
    static String ecmaAnchors(String regex) {
        StringBuilder out = new StringBuilder(regex.length() + 4);
        boolean inClass = false;
        for (int i = 0; i < regex.length(); i++) {
            char c = regex.charAt(i);
            if (c == '\\' && i + 1 < regex.length()) {
                out.append(c).append(regex.charAt(++i));
                continue;
            }
            if (inClass) {
                inClass = c != ']';
            } else if (c == '[') {
                inClass = true;
            } else if (c == '$') {
                out.append("\\z");
                continue;
            }
            out.append(c);
        }
        return out.toString();
    }
}
