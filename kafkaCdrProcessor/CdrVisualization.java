package com.trial.visualization;

import java.awt.Color;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class CdrVisualization extends ApplicationFrame {

	private static final long serialVersionUID = 1L;
	private static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");	
	}

	public CdrVisualization(final String title) throws Exception {

		super(title);

		final XYDataset dataset = fetchHBaseData();
		final JFreeChart chart = createChart(dataset);
		final ChartPanel chartPanel = new ChartPanel(chart);
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 500));
		setContentPane(chartPanel);

	}

	@SuppressWarnings({ "resource", "deprecation" })
	private XYDataset fetchHBaseData() throws Exception {

		final XYSeries series1 = new XYSeries("CDR data");
		try {
			HTable table = new HTable(conf, "cdrdata");
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);

			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					String cdrdata = new String(kv.getRow());	
					String op = cdrdata.split("-")[3];
					int opid = 0;
					if(op.equals("Airetl"))
							opid = 1;
					else if(op.equals("At&T"))
						opid = 2;
					else if(op.equals("Voda"))
						opid = 3;
					series1.add(opid,Double.parseDouble(new String(kv.getValue())));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		final XYSeriesCollection dataset = new XYSeriesCollection();
		dataset.addSeries(series1);

		return dataset;
	}

	private JFreeChart createChart(final XYDataset dataset) {

		final JFreeChart chart = ChartFactory.createXYLineChart(
				"Carrier Wise Traffic Report", "Account-Carrier", // x axis label
				"count", // y axis label
				dataset, // data
				PlotOrientation.VERTICAL, true, 
				true, 
				false 
				);

		
		chart.setBackgroundPaint(Color.white);

		final XYPlot plot = chart.getXYPlot();
	//	plot.setBackgroundPaint(Color.LIGHT_GRAY);
		plot.setBackgroundPaint(Color.BLUE);

		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.WHITE);


		final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
	//	renderer.setSeriesPaint(0, Color.BLACK);
		renderer.setSeriesPaint(0, Color.GREEN);
		renderer.setSeriesLinesVisible(0, true);
		plot.setRenderer(renderer);

		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

		return chart;

	}

	public static void main(final String[] args) throws Exception {

		for (int i = 0; i < 100; i++) {
			final CdrVisualization demo = new CdrVisualization("Carrier Wise Traffic Report");

			System.out.println("Refreshing");
			demo.repaint();
			demo.pack();


			RefineryUtilities.centerFrameOnScreen(demo);
			demo.setVisible(true);
			Thread.sleep(5000);
			demo.hide();
		}

	}

}
