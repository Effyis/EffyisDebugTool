package com.socialgist.debugtool.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.socialgist.gvp.utils.items.GvpUtils;
import com.socialgist.gvp.utils.items.StatsTimePeriod;

public class JsonData {
    private String cfTitle;
    private List<Column> cols;
    private List<Row> rows;

    public List<Column> getCols() {
        return cols;
    }

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public void buildTimeCollection(StatsTimePeriod chartTimePeriod) {

        List<Column> columns = new ArrayList<>();
        columns.add(new Column("year", "Year", "string"));
        setCols(columns);

        // Create and populate the "rows" list
        List<Row> rows = new ArrayList<>();
    	
        long adjustedTime = GvpUtils.adjustCurrentUnixTime(chartTimePeriod.getPeriodInSeconds());
    	long time = adjustedTime;
        int n = chartTimePeriod.getNumberOfPeriods();
        for (int i = 1; i <= n; i++) {
        	rows.add(new Row(time, chartTimePeriod.formatTime(time)));
        	time = time - chartTimePeriod.getPeriodInSeconds(); 
        }
        setRows(rows);
        
        Collections.reverse(rows);        
    }

	public void addMap(String title, Map<String, Long> map) {
		
		cols.add(new Column(title, title, "number"));
		
        Random random = new Random();
		for (Row row : rows) {
			Long value = map.getOrDefault(row.readTitle(), 0L);
//	        int randomNumber = random.nextInt(10) - 10 ;
	//        value = value + randomNumber;
			row.getC().add(new Cell(value));
		}
	}

	public void addColumn(String title, String type) {
		cols.add(new Column(title, title, type));
	}

	public Row addRow(Long id, String value) {
		Row row = new Row(id, value);
		rows.add(row);
		return row;
	}

	public Row addRow() {
		Row row = new Row();
		rows.add(row);
		return row;
	}
	
	
	public void buildCollections() {
        List<Column> columns = new ArrayList<>();
        setCols(columns);
        List<Row> rows = new ArrayList<>();
        setRows(rows);
	}    

}