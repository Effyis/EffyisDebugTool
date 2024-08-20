package com.socialgist.debugtool.model;

import java.util.ArrayList;
import java.util.List;

public class Row {
    private long time;
	private String title;
    private List<Cell> c;
    
	public Row(long time, String title) {
		this.setTime(time);
		this.setTitle(String.valueOf(time));
        c = new ArrayList<>();
        c.add(new Cell(title));
	}

	public Row() {
		this.setTime(0L);
		this.setTitle("");
        c = new ArrayList<>();
	}
	
	
	public List<Cell> getC() {
        return c;
    }

    public void setCells(List<Cell> c) {
        this.c = c;
    }

//	public long getTime() {
//		return time;
//	}

	public void setTime(long time) {
		this.time = time;
	}

//    public String getTitle() {
//		return title;
//	}

	public String readTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
}