package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: sebastiano
 * Date: 4/26/11
 * Time: 6:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class TPCCPopulation {

    private static Log log = LogFactory.getLog(TPCCPopulation.class);


    public static int NB_WAREHOUSE=1;

    private long POP_C_LAST = TPCCTools.NULL_NUMBER;
    private long POP_C_ID = TPCCTools.NULL_NUMBER;
    private long POP_OL_I_ID = TPCCTools.NULL_NUMBER;

    private boolean _new_order = false;
    private int _seqIdCustomer[];

    private CacheWrapper wrapper;

    private MemoryMXBean memoryBean;

    private int slaveIndex;
    private int numSlaves;


    public TPCCPopulation(CacheWrapper wrapper, int slaveIndex, int numSlaves) {

        this.wrapper=wrapper;


        this._seqIdCustomer = new int[TPCCTools.NB_MAX_CUSTOMER];

        this.memoryBean=ManagementFactory.getMemoryMXBean();

        this.slaveIndex=slaveIndex;
        this.numSlaves=numSlaves;


        populate_item();

        populate_wharehouse();

        System.gc();
    }


    public String c_last() {
        String c_last = "";
        long number = TPCCTools.nonUniformRandom(getC_LAST(), TPCCTools.A_C_LAST, TPCCTools.MIN_C_LAST, TPCCTools.MAX_C_LAST);
        String alea = String.valueOf(number);
        while (alea.length() < 3) {
            alea = "0"+alea;
        }
        for (int i=0; i<3; i++) {
            c_last += TPCCTools.C_LAST[Integer.parseInt(alea.substring(i, i+1))];
        }
        return c_last;
    }

    public long getC_LAST() {
        if (POP_C_LAST == TPCCTools.NULL_NUMBER) {
            POP_C_LAST = TPCCTools.randomNumber(TPCCTools.MIN_C_LAST, TPCCTools.A_C_LAST);
        }
        return POP_C_LAST;
    }

    public long getC_ID() {
        if (POP_C_ID == TPCCTools.NULL_NUMBER) {
            POP_C_ID = TPCCTools.randomNumber(0, TPCCTools.A_C_ID);
        }
        return POP_C_ID;
    }

    public long getOL_I_ID() {
        if (POP_OL_I_ID == TPCCTools.NULL_NUMBER) {
            POP_OL_I_ID = TPCCTools.randomNumber(0, TPCCTools.A_OL_I_ID);
        }
        return POP_OL_I_ID;
    }



    private void populate_item() {
        log.info("populate items");

        long init_id_item=1;
        long num_of_items=TPCCTools.NB_MAX_ITEM;

        if(numSlaves>1){
            num_of_items=TPCCTools.NB_MAX_ITEM/numSlaves;
            long reminder=TPCCTools.NB_MAX_ITEM % numSlaves;

            init_id_item=(slaveIndex*num_of_items)+1;

            if(slaveIndex==numSlaves-1){
                num_of_items+=reminder;
            }


        }
        log.info(" ITEM - ids="+init_id_item+",...,"+(init_id_item-1+num_of_items));
        for (long i = init_id_item; i <= (init_id_item-1+num_of_items); i++) {

            Item newItem = new Item(i, TPCCTools.alea_number(1, 10000), TPCCTools.alea_chainec(14, 24), TPCCTools.alea_float(1, 100, 2), TPCCTools.s_data());

            boolean successful=false;
            while (!successful){
                try {
                    newItem.store(wrapper);
                    successful=true;
                } catch (Throwable e) {
                    log.warn(e);
                }
            }


        }
        MemoryUsage u1=this.memoryBean.getHeapMemoryUsage();
        log.info("Memory Statistics (Heap) - used="+u1.getUsed()+" bytes; committed="+u1.getCommitted()+" bytes");
        MemoryUsage u2=this.memoryBean.getNonHeapMemoryUsage();
        log.info("Memory Statistics (NonHeap) - used="+u2.getUsed()+" bytes; committed="+u2.getCommitted()+" bytes");
    }

    private void populate_wharehouse() {
        log.info("populate wharehouses");
        if (NB_WAREHOUSE > 0) {
            for (int i = 1; i <= NB_WAREHOUSE; i++) {
                log.info(" WAREHOUSE " + i);
                if(this.slaveIndex==0){// Warehouse assigned to node 0 if I have more than one node
                    Warehouse newWarehouse= new Warehouse(i,
                            TPCCTools.alea_chainec(6, 10),
                            TPCCTools.alea_chainec(10, 20), TPCCTools.alea_chainec(10, 20),
                            TPCCTools.alea_chainec(10, 20), TPCCTools.alea_chainel(2, 2),
                            TPCCTools.alea_chainen(4, 4) + TPCCTools.CHAINE_5_1,
                            TPCCTools.alea_float(Float.valueOf("0.0000").floatValue(),
                                    Float.valueOf("0.2000").floatValue(), 4),
                            TPCCTools.WAREHOUSE_YTD);



                    boolean successful=false;
                    while (!successful){
                        try {
                            newWarehouse.store(wrapper);
                            successful=true;
                        } catch (Throwable e) {
                            log.warn(e);
                        }
                    }
                }
                populate_stock(i);

                populate_district(i);

                MemoryUsage u1=this.memoryBean.getHeapMemoryUsage();
                log.info("Memory Statistics (Heap) - used="+u1.getUsed()+" bytes; committed="+u1.getCommitted()+" bytes");
                MemoryUsage u2=this.memoryBean.getNonHeapMemoryUsage();
                log.info("Memory Statistics (NonHeap) - used="+u2.getUsed()+" bytes; committed="+u2.getCommitted()+" bytes");

            }
        }

    }

    private void populate_stock(int id_wharehouse) {
        log.info("populate stocks");
        if (id_wharehouse < 0) return;
        else {

            long init_id_item=1;
            long num_of_items=TPCCTools.NB_MAX_ITEM;

            if(numSlaves>1){
                num_of_items=TPCCTools.NB_MAX_ITEM/numSlaves;
                long reminder=TPCCTools.NB_MAX_ITEM % numSlaves;

                init_id_item=(slaveIndex*num_of_items)+1;

                if(slaveIndex==numSlaves-1){
                    num_of_items+=reminder;
                }


            }
            log.info(" STOCK for Warehouse "+id_wharehouse+" - ITEMS="+init_id_item+",...,"+(init_id_item-1+num_of_items));
            for (long i = init_id_item; i <= (init_id_item-1+num_of_items); i++) {

                Stock newStock=new Stock(i,
                        id_wharehouse,
                        TPCCTools.alea_number(10, 100),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        TPCCTools.alea_chainel(24, 24),
                        0,
                        0,
                        0,
                        TPCCTools.s_data());



                boolean successful=false;
                while (!successful){
                    try {
                        newStock.store(wrapper);
                        successful=true;
                    } catch (Throwable e) {
                        log.warn(e);
                    }
                }


            }
        }
    }

    private void populate_district(int id_wharehouse) {
        log.info("populate districts");
        if (id_wharehouse < 0) return;
        else {
            int init_id_district=1;
            int num_of_districts=TPCCTools.NB_MAX_DISTRICT;

            if(numSlaves>1){
                num_of_districts=TPCCTools.NB_MAX_DISTRICT/numSlaves;
                int reminder=TPCCTools.NB_MAX_DISTRICT % numSlaves;

                if(slaveIndex<=reminder){
                    init_id_district=(slaveIndex*(num_of_districts+1))+1;
                }
                else{
                    init_id_district=(((reminder)*(num_of_districts+1))+((slaveIndex-reminder)*num_of_districts))+1;
                }
                if(slaveIndex<reminder){
                    num_of_districts+=1;
                }

                log.info("Index:"+slaveIndex+"; Init"+init_id_district+"; Num:"+num_of_districts);
            }
            for (int id_district = init_id_district; id_district <= (init_id_district-1+num_of_districts); id_district++) {
                log.info(" DISTRICT " + id_district);

                District newDistrict = new District(id_wharehouse,
                        id_district,
                        TPCCTools.alea_chainec(6, 10),
                        TPCCTools.alea_chainec(10, 20),
                        TPCCTools.alea_chainec(10, 20),
                        TPCCTools.alea_chainec(10, 20),
                        TPCCTools.alea_chainel(2, 2),
                        TPCCTools.alea_chainen(4, 4) + TPCCTools.CHAINE_5_1,
                        TPCCTools.alea_float(Float.valueOf("0.0000").floatValue(), Float.valueOf("0.2000").floatValue(), 4),
                        TPCCTools.WAREHOUSE_YTD,
                        3001);



                boolean successful=false;
                while (!successful){
                    try {
                        newDistrict.store(wrapper);
                        successful=true;
                    } catch (Throwable e) {
                        log.warn(e);
                    }
                }


                populate_customer(id_wharehouse, id_district);
                populate_order(id_wharehouse, id_district);
            }
        }
    }

    private void populate_customer(int id_wharehouse, int id_district) {
        log.info("populate customer");
        if (id_wharehouse < 0 || id_district < 0) return;
        else {
            log.info(" CUSTOMER "+id_wharehouse+", "+id_district );
            for (int i = 1; i <= TPCCTools.NB_MAX_CUSTOMER; i++) {

                Customer newCustomer=new Customer(id_wharehouse,
                        id_district,
                        i,
                        TPCCTools.alea_chainec(8, 16),
                        "OE",
                        c_last(),
                        TPCCTools.alea_chainec(10, 20),
                        TPCCTools.alea_chainec(10, 20),
                        TPCCTools.alea_chainec(10, 20),
                        TPCCTools.alea_chainel(2, 2),
                        TPCCTools.alea_chainen(4, 4) + TPCCTools.CHAINE_5_1,
                        TPCCTools.alea_chainen(16, 16),
                        new Date(System.currentTimeMillis()),
                        (TPCCTools.alea_number(1, 10) == 1) ? "BC" : "GC",
                        500000.0, TPCCTools.alea_double(0., 0.5, 4), -10.0, 10.0, 1, 0, TPCCTools.alea_chainec(300, 500));



                boolean successful=false;
                while (!successful){
                    try {
                        newCustomer.store(wrapper);
                        successful=true;
                    } catch (Throwable e) {
                        log.warn(e);
                    }
                }


                populate_history(i, id_wharehouse, id_district);
            }
        }
    }

    private void populate_history(int id_customer, int id_wharehouse, int id_district) {
        log.info("populate history");
        if (id_customer < 0 || id_wharehouse < 0 || id_district < 0) return;
        else {

            History newHistory=new History(id_customer, id_district, id_wharehouse, id_district, id_wharehouse, new Date(System.currentTimeMillis()), 10, TPCCTools.alea_chainec(12, 24));



            boolean successful=false;
            while (!successful){
                try {
                    newHistory.store(wrapper);
                    successful=true;
                } catch (Throwable e) {
                    log.warn(e);
                }
            }

        }
    }


    private void populate_order(int id_wharehouse, int id_district) {
        log.info("populate order");
        this._new_order = false;
        log.info(" ORDER "+id_wharehouse+", "+id_district );
        for (int id_order = 1; id_order <= TPCCTools.NB_MAX_ORDER; id_order++) {

            int o_ol_cnt = TPCCTools.alea_number(5, 15);
            Date aDate = new Date((new java.util.Date()).getTime());

            Order newOrder= new Order(id_order,
                    id_district,
                    id_wharehouse,
                    generate_seq_alea(0, TPCCTools.NB_MAX_CUSTOMER-1),
                    aDate,
                    (id_order < TPCCTools.LIMIT_ORDER)?TPCCTools.alea_number(1, 10):0,
                    o_ol_cnt,
                    1);



            boolean successful=false;
            while (!successful){
                try {
                    newOrder.store(wrapper);
                    successful=true;
                } catch (Throwable e) {
                    log.warn(e);
                }
            }

            populate_order_line(id_wharehouse, id_district, id_order, o_ol_cnt, aDate);

            if (id_order >= TPCCTools.LIMIT_ORDER) populate_new_order(id_wharehouse, id_district, id_order);
        }
    }

    private void populate_order_line(int id_wharehouse, int id_district, int id_order, int o_ol_cnt, Date aDate) {
        log.info("populate order line");
        for (int i = 0; i < o_ol_cnt; i++) {

            double amount;
            Date delivery_date;

            if (id_order >= TPCCTools.LIMIT_ORDER) {
                amount=TPCCTools.alea_double(0.01, 9999.99, 2);
                delivery_date= null;
            }
            else {
                amount=0.0;
                delivery_date=aDate;
            }


            OrderLine newOrderLine= new OrderLine(id_order,
                    id_district,
                    id_wharehouse,
                    i,
                    TPCCTools.nonUniformRandom(getOL_I_ID(), TPCCTools.A_OL_I_ID, 1L, TPCCTools.NB_MAX_ITEM),
                    id_wharehouse,
                    delivery_date,
                    5,
                    amount,
                    TPCCTools.alea_chainel(12, 24));



            boolean successful=false;
            while (!successful){
                try {
                    newOrderLine.store(wrapper);
                    successful=true;
                } catch (Throwable e) {
                    log.warn(e);
                }
            }

        }
    }

    private void populate_new_order(int id_wharehouse, int id_district, int id_order) {
        log.info("populate new order");

        NewOrder newNewOrder= new NewOrder(id_order, id_district, id_wharehouse);



        boolean successful=false;
        while (!successful){
            try {
                newNewOrder.store(wrapper);
                successful=true;
            } catch (Throwable e) {
                log.warn(e);
            }
        }

    }


    private int generate_seq_alea(int deb, int fin) {
        if (!this._new_order) {
            for (int i = deb; i <= fin; i++) {
                this._seqIdCustomer[i] = i + 1;
            }
            this._new_order = true;
        }
        int rand = 0;
        int alea = 0;
        do {
            rand = (int) TPCCTools.nonUniformRandom(getC_ID(), TPCCTools.A_C_ID, deb, fin);
            alea = this._seqIdCustomer[rand];
        } while (alea == TPCCTools.NULL_NUMBER);
        _seqIdCustomer[rand] = TPCCTools.NULL_NUMBER;
        return alea;
    }





}
