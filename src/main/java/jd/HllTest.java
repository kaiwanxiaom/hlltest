package jd;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.clearspring.analytics.stream.cardinality.LogLog;
import jd.util.JedisAp;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYSeries;
import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.Jedis;

import java.awt.*;
import java.io.IOException;
import java.util.UUID;

public class HllTest {


    public static void main(String[] args) throws Exception {
//        System.out.println(1 << 14);

//        HyperLogLog lc = new HyperLogLog(14);
//        System.out.println(lc.getBytes().length);

        RedisHyperLogLogTest(1000 * 10000);
//        LinearCountingTest(100 * 10000);
    }

    private static void showM(double[] x, double[] y) throws IOException {

    }

    private static void show(double[] x, double[] y) throws IOException {
        XYChart chart = new XYChart(800, 500);
        chart.setTitle("");
        chart.setXAxisTitle("x");
        chart.getStyler().setYAxisMax(0.1);
        chart.setYAxisTitle("y");

//        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
//        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideSW);
        chart.getStyler().setMarkerSize(3);

        XYSeries series = chart.addSeries(" ", x, y);
//        series.setMarker(SeriesMarkers.DIAMOND);

        // Create Chart
//        XYChart chart = QuickChart.getChart("Sample Chart", "X", "Y", "y(x)", x, y);

// Show it
        new SwingWrapper(chart).displayChart();

// Save it
        BitmapEncoder.saveBitmap(chart, "./Sample_Chart", BitmapEncoder.BitmapFormat.PNG);

// or save it in high-res
        BitmapEncoder.saveBitmapWithDPI(chart, "./Sample_Chart_300_DPI", BitmapEncoder.BitmapFormat.PNG, 300);
    }

    private static void LinearCountingTest(int len) throws IOException {
        int mP = 14;
        XYChart chart = getDotsChart("LinearCounting " + "m=2^" + mP, "基数", "相对误差");
        double x[] = new double[len];
        double y[][] = new double[10][len];

        for(int k = 0; k < 1; k++) {
            LinearCounting lc = new LinearCounting(1<<(mP-3));

            for (int i = 1; i <= len; i++) {
                String s = UUID.randomUUID().toString().replace("-", "");
                lc.offer(s);

                y[k][i - 1] = (double) Math.abs(i - lc.cardinality()) / i;
                x[i - 1] = i;
            }

            XYSeries xySeries = chart.addSeries(" " + k, x, y[k]);
        }

        new SwingWrapper(chart).displayChart();

    }

    private static void LogLogCountingTest(int len) throws IOException {
        int m = 16;
        XYChart chart = getDotsChart("LogLogCounting " + "m = 2^" + m, "基数", "相对误差");
        double x[] = new double[len];
        double y[][] = new double[10][len];

        for(int k = 0; k < 5; k++) {
            LogLog lc = new LogLog(m);

            for (int i = 1; i <= len; i++) {
                String s = UUID.randomUUID().toString().substring(0, 18).replace("-", "");
                lc.offer(s);

                y[k][i - 1] = (double) Math.abs(i - lc.cardinality()) / i;
                x[i - 1] = i;
            }

            chart.addSeries(" " + k, x, y[k]);
        }

        new SwingWrapper(chart).displayChart();

    }

    private static void AdaptiveCountingTest(int len) throws IOException {
        int m = 16;
        XYChart chart = getDotsChart("AdaptiveCounting " + "m = 2^"+m, "基数", "相对误差");
        double x[] = new double[len];
        double y[][] = new double[10][len];

        for(int k = 0; k < 5; k++) {
            AdaptiveCounting lc = new AdaptiveCounting(m);

            for (int i = 1; i <= len; i++) {
                String s = UUID.randomUUID().toString().replace("-", "");
                lc.offer(s);

                y[k][i - 1] = (double) Math.abs(i - lc.cardinality()) / i;
                x[i - 1] = i;
                if (i % (len / 10) == 0) {
                    System.out.println(k + " " + i);
                }
            }

            chart.addSeries(" " + k, x, y[k]);
        }

        new SwingWrapper(chart).displayChart();

    }

    private static XYChart getDotsChart() {
        XYChart chart = new XYChart(800, 500);
        chart.setTitle("");
        chart.setXAxisTitle("x");
        chart.getStyler().setYAxisMax(0.1);
        chart.setYAxisTitle("y");

        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setMarkerSize(1);

        return chart;
    }

    private static XYChart getDotsChart(String title, String xTitle, String yTitle) {
        XYChart chart = new XYChart(500, 300);
        chart.setTitle(title);
        chart.setXAxisTitle(xTitle);
        chart.getStyler().setYAxisMax(0.1);
        chart.setYAxisTitle(yTitle);

//        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setMarkerSize(1);

        return chart;
    }

    private static void HyperLogLogTest(int len) throws Exception {

        int m = 16;
        XYChart chart = getDotsChart("HyperLogLog "+"m = 2^"+m, "基数", "相对误差");

        double x[] = new double[len];
        double y[][] = new double[10][len];

        for (int k = 0; k < 5; k++) {
            HyperLogLog lc = new HyperLogLog(m);

            for (int i = 1; i <= len; i++) {
                String s = UUID.randomUUID().toString().substring(0, 18).replace("-", "");
                lc.offer(s);
                y[k][i - 1] = (double) Math.abs(i - lc.cardinality()) / i;
                x[i - 1] = i;

                if (i % (len / 10) == 0) {
                    System.out.println(k + " " + i);
                }
            }

            chart.addSeries(" " + k, x, y[k]);
        }

        new SwingWrapper(chart).displayChart();

//        show(x, y);

    }

    private static void RedisHyperLogLogTest(int len) throws Exception {
        JedisAp jedisAp = new JedisAp();
        jedisAp.afterPropertiesSet();

        XYChart chart = getDotsChart("HyperLogLog Redis", "基数", "相对误差");

        double x[] = new double[len];
        double y[][] = new double[10][len];

        for (int k = 0; k < 5; k++) {
            String key = "hll" + k;

            for (int i = 1; i <= len; i++) {
                String s = UUID.randomUUID().toString().replace("-", "");
                jedisAp.pfadd(key, s);

                y[k][i - 1] = (double) Math.abs(i - jedisAp.pfcount(key)) / i;
                x[i - 1] = i;

                if (i % (len / 10) == 0) {
                    System.out.println(k + " " + i);
                }
            }

            chart.addSeries(" " + k, x, y[k]);
        }

        new SwingWrapper(chart).displayChart();

//        show(x, y);

    }
}
