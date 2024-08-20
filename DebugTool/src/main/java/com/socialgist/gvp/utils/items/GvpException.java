package com.socialgist.gvp.utils.items;

public class GvpException extends Exception {

	private static final long serialVersionUID = 1L;

    public enum GvpExceptionType {
        REQUEST_NOT_VALID, 
        REQUEST_DUPLICATE, 
        APICLIENT_ERROR, 
        NOT_DEFINED
    }    
    public GvpExceptionType type;

	public GvpException(GvpExceptionType type, String message) {
		super(message);
		this.type = type;
	}
	
//    private static String composeMessage(GvpExceptionType type, String message) {
//    	StringBuilder result = new StringBuilder(type.toString()).append(message);
//    	return result.toString();
//	}
	
	
	
}
