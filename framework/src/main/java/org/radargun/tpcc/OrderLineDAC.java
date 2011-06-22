package org.radargun.tpcc;

import org.radargun.CacheWrapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: sebastiano
 * Date: 28/04/11
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
public class OrderLineDAC {

    private OrderLineDAC(){}

    public static List<OrderLine> loadByOrder(CacheWrapper cacheWrapper, Order order) throws Throwable{

        List<OrderLine> list=new ArrayList<OrderLine>();

        if(order==null) return list;

        int numLines=order.getO_ol_cnt();

        OrderLine current=null;
        boolean found=false;

        for(int i=0; i<numLines; i++){

            current= new OrderLine();
            current.setOl_w_id(order.getO_w_id());
            current.setOl_d_id(order.getO_d_id());
            current.setOl_o_id(order.getO_id());
            current.setOl_number(i);

            found=current.load(cacheWrapper);

            if(found) list.add(current);

        }

        return list;

    }
}
