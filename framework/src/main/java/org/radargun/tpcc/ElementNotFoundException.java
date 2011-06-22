package org.radargun.tpcc;

/**
 * Created by IntelliJ IDEA.
 * User: sebastiano
 * Date: 29/04/11
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
public class ElementNotFoundException extends Exception{

    public ElementNotFoundException(){
        super();
    }
    public ElementNotFoundException(String message){
        super(message);
    }
    public ElementNotFoundException(String message, Throwable cause){
        super(message, cause);
    }
    public ElementNotFoundException(Throwable cause){
        super(cause);
    }
}
