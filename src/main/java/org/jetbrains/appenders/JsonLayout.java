/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.appenders;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.AppenderAttachable;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class JsonLayout extends Layout {

    private static final Pattern SEP_PATTERN = Pattern.compile("(?:\\p{Space}*?[,;]\\p{Space}*)+");
    private static final Pattern PAIR_SEP_PATTERN = Pattern.compile("(?:\\p{Space}*?[:=]\\p{Space}*)+");

    private static final char[] HEX_CHARS =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private class FieldLabels {
        private String locationFieldKey = "location";

        private Map<String, String> defaultFieldLabels = new HashMap<String, String>();
        private Map<String, String> usedFieldLabels = new HashMap<String, String>();

        private void setDefaultFieldLabels() {
            defaultFieldLabels.put("exception", "exception");
            defaultFieldLabels.put("exception.class", "class");
            defaultFieldLabels.put("exception.message", "message");
            defaultFieldLabels.put("exception.stacktrace", "stacktrace");

            defaultFieldLabels.put("level", "level");
            defaultFieldLabels.put("location", "location");
            defaultFieldLabels.put("logger", "logger");
            defaultFieldLabels.put("message", "message");
            defaultFieldLabels.put("mdc", "mdc");
            defaultFieldLabels.put("ndc", "ndc");
            defaultFieldLabels.put("host", "host");
            defaultFieldLabels.put("path", "path");
            defaultFieldLabels.put("tags", "tags");
            defaultFieldLabels.put("@timestamp", "@timestamp");
            defaultFieldLabels.put("thread", "thread");
            defaultFieldLabels.put("@version", "@version");
        }

        FieldLabels() {
            setDefaultFieldLabels();
            for(String key : defaultFieldLabels.keySet()) {
                add(key, defaultFieldLabels.get(key));
            }

            remove(locationFieldKey);
        }

        private void update(String key, String value) {
            usedFieldLabels.put(key, value);
        }

        private void remove(String key) {
            if(usedFieldLabels.containsKey(key)) {
                usedFieldLabels.remove(key);
            }

            if(key.startsWith(locationFieldKey)) {
                usedFieldLabels.remove("location.class");
                usedFieldLabels.remove("location.file");
                usedFieldLabels.remove("location.line");
                usedFieldLabels.remove("location.method");
            }

            Map<String, String> x = usedFieldLabels;
        }

        private void add(String key) {
            if(defaultFieldLabels.containsKey(key)) {
                add(key, defaultFieldLabels.get(key));
            }

            if(key.startsWith(locationFieldKey)) {
                add("location.class", "class");
                add("location.file", "file");
                add("location.line", "line");
                add("location.method", "method");
            }
        }

        private void add(String key, String value) {
            usedFieldLabels.put(key, value);
        }

        private String getFieldLabel(String key) {
            if(usedFieldLabels.containsKey(key)) {
                return usedFieldLabels.get(key);
            } else {
                return null;
            }
        }

        private String getLocationFieldLabel(String key) {
            return getFieldLabel("location." + key);
        }

        private String getExceptionFieldLabel(String key) {
            return getFieldLabel("exception." + key);
        }
    }

    private class RenderedFieldLabels {
        String locationClass = fieldLabels.getLocationFieldLabel("class");
        String locationFile = fieldLabels.getLocationFieldLabel("file");
        String locationLine = fieldLabels.getLocationFieldLabel("line");
        String locationMethod = fieldLabels.getLocationFieldLabel("method");

        String exceptionClass = fieldLabels.getExceptionFieldLabel("class");
        String exceptionMessage = fieldLabels.getExceptionFieldLabel("message");
        String exceptionStacktrace = fieldLabels.getExceptionFieldLabel("stacktrace");

        String exception = fieldLabels.getFieldLabel("exception");
        String level = fieldLabels.getFieldLabel("level");
        String location = fieldLabels.getFieldLabel("location");
        String logger = fieldLabels.getFieldLabel("logger");
        String message = fieldLabels.getFieldLabel("message");
        String mdc = fieldLabels.getFieldLabel("mdc");
        String ndc = fieldLabels.getFieldLabel("ndc");
        String host = fieldLabels.getFieldLabel("host");
        String path = fieldLabels.getFieldLabel("path");
        String tags = fieldLabels.getFieldLabel("tags");
        String timestamp = fieldLabels.getFieldLabel("@timestamp");
        String thread = fieldLabels.getFieldLabel("thread");
        String version = fieldLabels.getFieldLabel("@version");
    }

    private static final String VERSION = "1";

    private String tagsVal;
    private String fieldsVal;
    private String includedFields;
    private String excludedFields;
    private String[] renamedFieldLabels;

    private final Map<String, String> fields;
    private FieldLabels fieldLabels = new FieldLabels();
    private RenderedFieldLabels renderedFieldLabels = new RenderedFieldLabels();

    private final DateFormat dateFormat;
    private final Date date;
    private final StringBuilder buf;

    private String[] tags;
    private String path;
    private boolean pathResolved;
    private String hostName;
    private boolean ignoresThrowable;

    public JsonLayout() {
        fields = new HashMap<String, String>();

        dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        date = new Date();
        buf = new StringBuilder(32*1024);
    }

    @Override
    public String format(LoggingEvent event) {
        buf.setLength(0);

        buf.append('{');

        boolean hasPrevField = false;
        if (renderedFieldLabels.exception != null) {
            hasPrevField = appendException(buf, event);
        }

        if (hasPrevField) {
            buf.append(',');
        }
        hasPrevField = appendFields(buf, event);

        if (renderedFieldLabels.level !=  null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.level, event.getLevel().toString());
            hasPrevField = true;
        }

        if (renderedFieldLabels.location != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendLocation(buf, event);
        }

        if (renderedFieldLabels.logger != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.logger, event.getLoggerName());
            hasPrevField = true;
        }

        if (renderedFieldLabels.message != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.message, event.getRenderedMessage());
            hasPrevField = true;
        }

        if (renderedFieldLabels.mdc != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendMDC(buf, event);
        }

        if (renderedFieldLabels.ndc != null) {
            String ndc = event.getNDC();
            if (ndc != null && !ndc.isEmpty()) {
                if (hasPrevField) {
                    buf.append(',');
                }
                appendField(buf, renderedFieldLabels.ndc, event.getNDC());
                hasPrevField = true;
            }
        }

        if (renderedFieldLabels.host != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.host, hostName);
            hasPrevField = true;
        }

        if (renderedFieldLabels.path != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendSourcePath(buf, event);
        }

        if (renderedFieldLabels.tags != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendTags(buf, event);
        }

        if (renderedFieldLabels.timestamp != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            date.setTime(event.getTimeStamp());
            appendField(buf, renderedFieldLabels.timestamp, dateFormat.format(date));
            hasPrevField = true;
        }

        if (renderedFieldLabels.thread != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.thread, event.getThreadName());
            hasPrevField = true;
        }

        if (renderedFieldLabels.version != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.version, VERSION);
        }

        buf.append("}\n");

        return buf.toString();
    }

    @SuppressWarnings("UnusedParameters")
    private boolean appendFields(StringBuilder buf, LoggingEvent event) {
        if (fields.isEmpty()) {
            return false;
        }

        for (Iterator<Map.Entry<String, String>> iter = fields.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, String> entry = iter.next();
            appendField(buf, entry.getKey(), entry.getValue());
            if (iter.hasNext()) {
                buf.append(',');
            }
        }

        return true;
    }

    private boolean appendSourcePath(StringBuilder buf, LoggingEvent event) {
        if (!pathResolved) {
            @SuppressWarnings("unchecked")
            Appender appender = findLayoutAppender(event.getLogger());
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;
                path = getAppenderPath(fileAppender);
            }
            pathResolved = true;
        }
        if (path != null) {
            appendField(buf, renderedFieldLabels.path, path);
            return true;
        }
        return false;
    }

    private Appender findLayoutAppender(Category logger) {
        for(Category parent = logger; parent != null; parent = parent.getParent()) {
            @SuppressWarnings("unchecked")
            Appender appender = findLayoutAppender(parent.getAllAppenders());
            if(appender != null) {
                return appender;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Appender findLayoutAppender(Enumeration<? extends Appender> appenders) {
        if(appenders == null) {
            return null;
        }

        while (appenders.hasMoreElements()) {
            Appender appender = appenders.nextElement();
            // get the first appender with this layout instance and ignore others;
            // actually a single instance of this class is not intended to be used with multiple threads.
            if (appender.getLayout() == this) {
                return appender;
            }
            if (appender instanceof AppenderAttachable) {
                AppenderAttachable appenderContainer = (AppenderAttachable) appender;
                return findLayoutAppender(appenderContainer.getAllAppenders());
            }
        }
        return null;
    }

    private String getAppenderPath(FileAppender fileAppender) {
        String path = null;
        try {
            String fileName = fileAppender.getFile();
            if (fileName != null && !fileName.isEmpty()) {
                path = new File(fileName).getCanonicalPath();
            }
        } catch (IOException e) {
            LogLog.error("Unable to retrieve appender's file name", e);
        }
        return path;
    }

    @SuppressWarnings("UnusedParameters")
    private boolean appendTags(StringBuilder builder, LoggingEvent event) {
        if (tags == null || tags.length == 0) {
            return false;
        }

        appendQuotedName(builder, renderedFieldLabels.tags);
        builder.append(":[");
        for (int i = 0, len = tags.length; i < len; i++) {
            appendQuotedValue(builder, tags[i]);
            if (i != len - 1) {
                builder.append(',');
            }
        }
        builder.append(']');

        return true;
    }

    private boolean appendMDC(StringBuilder buf, LoggingEvent event) {
        Map<?, ?> entries = event.getProperties();
        if (entries.isEmpty()) {
            return false;
        }

        appendQuotedName(buf, renderedFieldLabels.mdc);
        buf.append(":{");

        for (Iterator<? extends Map.Entry<?, ?>> iter = entries.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<?, ?> entry = iter.next();
            appendField(buf, entry.getKey(), entry.getValue());
            if (iter.hasNext()) {
                buf.append(',');
            }
        }
        buf.append('}');

        return true;
    }

    private boolean appendLocation(StringBuilder buf, LoggingEvent event) {
        LocationInfo locationInfo = event.getLocationInformation();
        if (locationInfo == null) {
            return false;
        }

        boolean hasPrevField = false;

        appendQuotedName(buf, renderedFieldLabels.location);
        buf.append(":{");

        String className = locationInfo.getClassName();
        if (className != null) {
            appendField(buf, renderedFieldLabels.locationClass, className);
            hasPrevField = true;
        }

        String fileName = locationInfo.getFileName();
        if (fileName != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.locationFile, fileName);
            hasPrevField = true;
        }

        String methodName = locationInfo.getMethodName();
        if (methodName != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.locationMethod, methodName);
            hasPrevField = true;
        }

        String lineNum = locationInfo.getLineNumber();
        if (lineNum != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, renderedFieldLabels.locationLine, lineNum);
        }

        buf.append('}');

        return true;
    }

    private boolean appendException(StringBuilder buf, LoggingEvent event) {
        ThrowableInformation throwableInfo = event.getThrowableInformation();
        if (throwableInfo == null) {
            return false;
        }

        appendQuotedName(buf, renderedFieldLabels.exception);
        buf.append(":{");

        boolean hasPrevField = false;

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        Throwable throwable = throwableInfo.getThrowable();
        if (throwable != null) {
            String message = throwable.getMessage();
            if (message != null) {
                appendField(buf, renderedFieldLabels.exceptionMessage, message);
                hasPrevField = true;
            }

            String className = throwable.getClass().getCanonicalName();
            if (className != null) {
                if (hasPrevField) {
                    buf.append(',');
                }
                appendField(buf, renderedFieldLabels.exceptionClass, className);
                hasPrevField = true;
            }
        }

        String[] stackTrace = throwableInfo.getThrowableStrRep();
        if (stackTrace != null && stackTrace.length != 0) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendQuotedName(buf, renderedFieldLabels.exceptionStacktrace);
            buf.append(":\"");
            for (int i = 0, len = stackTrace.length; i < len; i++) {
                appendValue(buf, stackTrace[i]);
                if (i != len - 1) {
                    appendChar(buf, '\n');
                }
            }
            buf.append('\"');
        }

        buf.append('}');

        return true;
    }

    @Override
    public boolean ignoresThrowable() {
        return ignoresThrowable;
    }

    public void activateOptions() {
        if (includedFields != null) {
            String[] included = SEP_PATTERN.split(includedFields);
            for (String val : included) {
                fieldLabels.add(val);
            }
        }
        if(renamedFieldLabels != null) {
            for(String fieldLabel : renamedFieldLabels) {
                String[] field = PAIR_SEP_PATTERN.split(fieldLabel);
                fieldLabels.update(field[0], field[1]);
            }
        }
        if (excludedFields != null) {
            String[] excluded = SEP_PATTERN.split(excludedFields);
            for (String val : excluded) {
                fieldLabels.remove(val);
            }
        }
        if (tagsVal != null) {
            tags = SEP_PATTERN.split(tagsVal);
        }
        if (fieldsVal != null) {
            String[] fields = SEP_PATTERN.split(fieldsVal);
            for (String fieldVal : fields) {
                String[] field = PAIR_SEP_PATTERN.split(fieldVal);
                this.fields.put(field[0], field[1]);
            }
        }
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostName = "localhost";
                LogLog.error("Unable to determine name of the localhost", e);
            }
        }
        ignoresThrowable = renderedFieldLabels.exception == null;
        renderedFieldLabels = new RenderedFieldLabels();
    }

    @Override
    public String getContentType() {
        return "application/json";
    }

    private void appendQuotedName(StringBuilder out, Object name) {
        out.append('\"');
        appendValue(out, String.valueOf(name));
        out.append('\"');
    }

    private void appendQuotedValue(StringBuilder out, Object val) {
        out.append('\"');
        appendValue(out, String.valueOf(val));
        out.append('\"');
    }

    private void appendValue(StringBuilder out, String val) {
        for (int i = 0, len = val.length(); i < len; i++) {
            appendChar(out, val.charAt(i));
        }
    }

    private void appendField(StringBuilder out, Object name, Object val) {
        appendQuotedName(out, name);
        out.append(':');
        appendQuotedValue(out, val);
    }

    private void appendChar(StringBuilder out, char ch) {
        switch (ch) {
            case '"':
                out.append("\\\"");
                break;
            case '\\':
                out.append("\\\\");
                break;
            case '/':
                out.append("\\/");
                break;
            case '\b':
                out.append("\\b");
                break;
            case '\f':
                out.append("\\f");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                if ((ch <= '\u001F') || ('\u007F' <= ch && ch <= '\u009F') || ('\u2000' <= ch && ch <= '\u20FF')) {
                    out.append("\\u")
                        .append(HEX_CHARS[ch >> 12 & 0x000F])
                        .append(HEX_CHARS[ch >> 8 & 0x000F])
                        .append(HEX_CHARS[ch >> 4 & 0x000F])
                        .append(HEX_CHARS[ch & 0x000F]);
                } else {
                    out.append(ch);
                }
                break;
        }
    }

    public void setTags(String tags) {
        this.tagsVal = tags;
    }

    public void setFields(String fields) {
        this.fieldsVal = fields;
    }

    public void setIncludedFields(String includedFields) {
        this.includedFields = includedFields;
    }

    public void setExcludedFields(String excludedFields) {
        this.excludedFields = excludedFields;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setRenamedFieldLabels(String renamedFieldLabels) {
        this.renamedFieldLabels = SEP_PATTERN.split(renamedFieldLabels);
    }
}
