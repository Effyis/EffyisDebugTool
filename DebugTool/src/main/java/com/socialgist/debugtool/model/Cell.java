package com.socialgist.debugtool.model;

public class Cell {
    private Object v;

    public Cell(Object v) {
		super();
		this.v = v;
	}

	public Cell(String v2) {
		this.v = v2;
	}

	public Cell(long v2) {
		this.v = v2;
	}

	public Object getV() {
        return v;
    }

    public void setV(Object v) {
        this.v = v;
    }
}