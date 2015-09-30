
package org.xbib.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class XContentMapValues {

    /**
     * Extracts raw values (string, int, and so on) based on the path provided returning all of them
     * as a single list.
     */
    public static List<Object> extractRawValues(String path, Map<String, Object> map) {
        List<Object> values = new ArrayList<>();
        String[] pathElements = Strings.splitStringToArray(path, '.');
        if (pathElements.length == 0) {
            return values;
        }
        extractRawValues(values, map, pathElements, 0);
        return values;
    }

    @SuppressWarnings({"unchecked"})
    private static void extractRawValues(List values, Map<String, Object> part, String[] pathElements, int index) {
        if (index == pathElements.length) {
            return;
        }
        if (part == null) {
            return;
        }

        String key = pathElements[index];
        Object currentValue = part.get(key);
        int nextIndex = index + 1;
        while (currentValue == null && nextIndex != pathElements.length) {
            key += "." + pathElements[nextIndex];
            currentValue = part.get(key);
            nextIndex++;
        }

        if (currentValue == null) {
            return;
        }

        if (currentValue instanceof Map) {
            extractRawValues(values, (Map<String, Object>) currentValue, pathElements, nextIndex);
        } else if (currentValue instanceof List) {
            extractRawValues(values, (List) currentValue, pathElements, nextIndex);
        } else {
            values.add(currentValue);
        }
    }

    @SuppressWarnings({"unchecked"})
    private static void extractRawValues(List values, List<Object> part, String[] pathElements, int index) {
        for (Object value : part) {
            if (value == null) {
                continue;
            }
            if (value instanceof Map) {
                extractRawValues(values, (Map<String, Object>) value, pathElements, index);
            } else if (value instanceof List) {
                extractRawValues(values, (List) value, pathElements, index);
            } else {
                values.add(value);
            }
        }
    }

    public static Object extractValue(String path, Map<String, Object> map) {
        String[] pathElements = Strings.splitStringToArray(path, '.');
        if (pathElements.length == 0) {
            return null;
        }
        return extractValue(pathElements, 0, map);
    }

    @SuppressWarnings({"unchecked"})
    private static Object extractValue(String[] pathElements, int index, Object currentValue) {
        if (index == pathElements.length) {
            return currentValue;
        }
        if (currentValue == null) {
            return null;
        }
        if (currentValue instanceof Map) {
            Map map = (Map) currentValue;
            String key = pathElements[index];
            Object mapValue = map.get(key);
            int nextIndex = index + 1;
            while (mapValue == null && nextIndex != pathElements.length) {
                key += "." + pathElements[nextIndex];
                mapValue = map.get(key);
                nextIndex++;
            }
            return extractValue(pathElements, nextIndex, mapValue);
        }
        if (currentValue instanceof List) {
            List valueList = (List) currentValue;
            List newList = new ArrayList(valueList.size());
            for (Object o : valueList) {
                Object listValue = extractValue(pathElements, index, o);
                if (listValue != null) {
                    newList.add(listValue);
                }
            }
            return newList;
        }
        return null;
    }

    public static boolean isObject(Object node) {
        return node instanceof Map;
    }

    public static boolean isArray(Object node) {
        return node instanceof List;
    }

    public static String nodeStringValue(Object node, String defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return node.toString();
    }

    public static float nodeFloatValue(Object node, float defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeFloatValue(node);
    }

    public static float nodeFloatValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).floatValue();
        }
        return Float.parseFloat(node.toString());
    }

    public static double nodeDoubleValue(Object node, double defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeDoubleValue(node);
    }

    public static double nodeDoubleValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).doubleValue();
        }
        return Double.parseDouble(node.toString());
    }

    public static int nodeIntegerValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).intValue();
        }
        return Integer.parseInt(node.toString());
    }

    public static int nodeIntegerValue(Object node, int defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        if (node instanceof Number) {
            return ((Number) node).intValue();
        }
        return Integer.parseInt(node.toString());
    }

    public static short nodeShortValue(Object node, short defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeShortValue(node);
    }

    public static short nodeShortValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).shortValue();
        }
        return Short.parseShort(node.toString());
    }

    public static byte nodeByteValue(Object node, byte defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeByteValue(node);
    }

    public static byte nodeByteValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).byteValue();
        }
        return Byte.parseByte(node.toString());
    }

    public static long nodeLongValue(Object node, long defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeLongValue(node);
    }

    public static long nodeLongValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).longValue();
        }
        return Long.parseLong(node.toString());
    }

    public static boolean nodeBooleanValue(Object node, boolean defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeBooleanValue(node);
    }

    public static boolean nodeBooleanValue(Object node) {
        if (node instanceof Boolean) {
            return (Boolean) node;
        }
        if (node instanceof Number) {
            return ((Number) node).intValue() != 0;
        }
        String value = node.toString();
        return !(value.equals("false") || value.equals("0") || value.equals("off"));
    }

    public static TimeValue nodeTimeValue(Object node, TimeValue defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeTimeValue(node);
    }

    public static TimeValue nodeTimeValue(Object node) {
        if (node instanceof Number) {
            return TimeValue.timeValueMillis(((Number) node).longValue());
        }
        return TimeValue.parseTimeValue(node.toString(), null);
    }
}
