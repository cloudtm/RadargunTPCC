package org.radargun.tpcc;

import org.radargun.CacheWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: sebastiano
 * Date: 28/04/11
 * Time: 15:20
 * To change this template use File | Settings | File Templates.
 */
public final class OrderDAC {

    private OrderDAC(){}

    public static Order loadByGreatestId(CacheWrapper cacheWrapper, long w_id, long d_id, long c_id) throws Throwable{


        List<Order> list=new ArrayList<Order>();
        boolean found=false;
        Order current=null;

        for (int id_order = 1; id_order <= TPCCTools.NB_MAX_ORDER; id_order++) {

            current=new Order();

            current.setO_id(id_order);
            current.setO_w_id(w_id);
            current.setO_d_id(d_id);

            found=current.load(cacheWrapper);

            if(found && current.getO_c_id()==c_id){

                list.add(current);
            }



        }

        if(list.isEmpty()) return null;

        Collections.sort(list);  // Decreasing order of o_id

        return list.iterator().next();



    }
}
