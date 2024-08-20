package com.socialgist.gvp.utils.items;

public enum GvpMessageResultType {

	    SUCCESS("success"),
	    ERROR("error"),
	    HB("hb");

	    private final String type;

	    GvpMessageResultType(String typeString) {
	        this.type = typeString;
	    }

	    public String getTypeString() {
	        return type;
	    }
}
