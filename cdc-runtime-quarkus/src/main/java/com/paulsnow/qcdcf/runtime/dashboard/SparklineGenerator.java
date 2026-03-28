package com.paulsnow.qcdcf.runtime.dashboard;

import java.util.List;
import java.util.Locale;

/**
 * Generates inline SVG sparkline charts from a list of data points.
 * <p>
 * No external chart library is required — the output is a self-contained SVG
 * element suitable for embedding directly in HTML.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
public final class SparklineGenerator {

    private SparklineGenerator() {
        // utility class
    }

    /**
     * Generates an inline SVG sparkline from data points.
     *
     * @param data   the data points
     * @param width  SVG width in pixels
     * @param height SVG height in pixels
     * @param colour stroke colour (e.g. {@code "#818cf8"} for indigo)
     * @return SVG markup string
     */
    public static String generate(List<Double> data, int width, int height, String colour) {
        if (data == null || data.isEmpty()) {
            return emptySparkline(width, height, colour);
        }

        double max = data.stream().mapToDouble(d -> d).max().orElse(1.0);
        if (max == 0) {
            max = 1.0;
        }

        double xStep = (double) width / Math.max(1, data.size() - 1);

        StringBuilder path = new StringBuilder();
        StringBuilder areaPath = new StringBuilder();

        for (int i = 0; i < data.size(); i++) {
            double x = i * xStep;
            double y = height - (data.get(i) / max * (height - 4)) - 2;

            if (i == 0) {
                path.append(String.format(Locale.ROOT, "M %.1f %.1f", x, y));
                areaPath.append(String.format(Locale.ROOT, "M %.1f %d", x, height));
                areaPath.append(String.format(Locale.ROOT, " L %.1f %.1f", x, y));
            } else {
                path.append(String.format(Locale.ROOT, " L %.1f %.1f", x, y));
                areaPath.append(String.format(Locale.ROOT, " L %.1f %.1f", x, y));
            }
        }
        areaPath.append(String.format(Locale.ROOT, " L %.1f %d Z", (data.size() - 1) * xStep, height));

        return String.format(Locale.ROOT, """
                <svg width="%d" height="%d" xmlns="http://www.w3.org/2000/svg">\
                <path d="%s" fill="%s" fill-opacity="0.1"/>\
                <path d="%s" fill="none" stroke="%s" stroke-width="1.5" \
                stroke-linecap="round" stroke-linejoin="round"/>\
                </svg>""", width, height, areaPath, colour, path, colour);
    }

    /**
     * Returns an empty sparkline (flat line) for when no data is available.
     */
    private static String emptySparkline(int width, int height, String colour) {
        int y = height - 2;
        return String.format(Locale.ROOT, """
                <svg width="%d" height="%d" xmlns="http://www.w3.org/2000/svg">\
                <line x1="0" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="1" \
                stroke-opacity="0.3"/>\
                </svg>""", width, height, y, width, y, colour);
    }
}
