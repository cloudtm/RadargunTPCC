package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;

import java.security.AllPermission;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: sebastiano
 * Date: 4/27/11
 * Time: 2:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class TPCCTerminal {

    private static Log log = LogFactory.getLog(TPCCTerminal.class);

    public final static int NEW_ORDER = 1, PAYMENT = 2, ORDER_STATUS = 3, DELIVERY = 4, STOCK_LEVEL = 5;
    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
    public static double PAYMENT_WEIGHT=43.0;
    public static double ORDER_STATUS_WEIGHT=4.0;


    private long c_run;
    private long c_id;
    private long ol_i_id;

    public TPCCTerminal(long c_run, long c_id, long ol_i_id){


        this.c_run=c_run;
        this.c_id=c_id;
        this.ol_i_id=ol_i_id;
    }

    /*
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable{


        long transactionType = TPCCTools.randomNumber(1, 100);

        if(transactionType <= paymentWeight){
             executeTransaction(cacheWrapper, PAYMENT);
        }
        else if(transactionType <= paymentWeight + orderStatusWeight){
                executeTransaction(cacheWrapper, ORDER_STATUS);

        }
        else{
             executeTransaction(cacheWrapper, NEW_ORDER);
        }



    }
    */

    public int choiceTransaction(boolean isPassiveReplication, boolean isPrimary){


        double newPayment_weight, oldPayment_weight;
        double oldNewOrder_weight;

         double transactionType = TPCCTools.doubleRandomNumber(0,100);

        if(!isPassiveReplication){
            if(transactionType <= PAYMENT_WEIGHT){
                 return PAYMENT;
            }
            else if(transactionType <= PAYMENT_WEIGHT + ORDER_STATUS_WEIGHT){
                    return ORDER_STATUS;

            }
            else{
                 return NEW_ORDER;
            }
        }
        else{
            if(isPrimary){ //All write on Primary
                oldPayment_weight=PAYMENT_WEIGHT;
                oldNewOrder_weight=100.0-(PAYMENT_WEIGHT+ORDER_STATUS_WEIGHT);

                newPayment_weight=((oldPayment_weight)/(oldPayment_weight+oldNewOrder_weight) ) *100.0;


                if(transactionType <= newPayment_weight){
                 return PAYMENT;
                }
                else{
                    return NEW_ORDER;
                }

            }
            else{//All read-only on Backups
                return ORDER_STATUS;
            }
        }
    }



    public void executeTransaction(CacheWrapper cacheWrapper, int transaction)  throws Throwable
    {
        long terminalWarehouseID=TPCCTools.randomNumber(1,TPCCPopulation.NB_WAREHOUSE);

        switch(transaction)
        {
            case NEW_ORDER:
                long districtID = TPCCTools.randomNumber(1, 10);
                long customerID = TPCCTools.nonUniformRandom(this.c_id, TPCCTools.A_C_ID, 1, TPCCTools.NB_MAX_CUSTOMER);

                int numItems = (int)TPCCTools.randomNumber(5, 15); // o_ol_cnt
                long[] itemIDs = new long[numItems];
                long[] supplierWarehouseIDs = new long[numItems];
                long[] orderQuantities = new long[numItems];
                int allLocal = 1; // see clause 2.4.2.2 (dot 6)
                for(int i = 0; i < numItems; i++) // clause 2.4.1.5
                {
                    itemIDs[i] = TPCCTools.nonUniformRandom(this.ol_i_id, TPCCTools.A_OL_I_ID, 1, TPCCTools.NB_MAX_ITEM);
                    if(TPCCTools.randomNumber(1, 100) > 1)
                    {
                        supplierWarehouseIDs[i] = terminalWarehouseID;
                    }
                    else //see clause 2.4.1.5 (dot 2)
                    {
                        do
                        {
                            supplierWarehouseIDs[i] = TPCCTools.randomNumber(1, TPCCPopulation.NB_WAREHOUSE);
                        }
                        while(supplierWarehouseIDs[i] == terminalWarehouseID && TPCCPopulation.NB_WAREHOUSE > 1);
                        allLocal = 0;// see clause 2.4.2.2 (dot 6)
                    }
                    orderQuantities[i] = TPCCTools.randomNumber(1, 10); //see clause 2.4.1.5 (dot 6)
                }
                // clause 2.4.1.5 (dot 1)
                //if(TPCCTools.randomNumber(1, 100) == 1)
                    //itemIDs[numItems-1] = -12345;

                newOrderTransaction(cacheWrapper, terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities);
                break;


            case PAYMENT:
                districtID = TPCCTools.randomNumber(1, 10);

                long x = TPCCTools.randomNumber(1, 100);
                long customerDistrictID;
                long customerWarehouseID;
                if(x <= 85)
                {
                    customerDistrictID = districtID;
                    customerWarehouseID = terminalWarehouseID;
                }
                else
                {
                    customerDistrictID = TPCCTools.randomNumber(1, 10);
                    do
                    {
                        customerWarehouseID = TPCCTools.randomNumber(1, TPCCPopulation.NB_WAREHOUSE);
                    }
                    while(customerWarehouseID == terminalWarehouseID && TPCCPopulation.NB_WAREHOUSE > 1);
                }

                long y = TPCCTools.randomNumber(1, 100);
                boolean customerByName;
                String customerLastName = null;
                customerID = -1;
                if(y <= 60)
                {
                    customerByName = true;
                    customerLastName = lastName((int)TPCCTools.nonUniformRandom(this.c_run, TPCCTools.A_C_LAST, 0, TPCCTools.MAX_C_LAST));
                }
                else
                {
                    customerByName = false;
                    customerID = TPCCTools.nonUniformRandom(this.c_id, TPCCTools.A_C_ID, 1, TPCCTools.NB_MAX_CUSTOMER);
                }

                double paymentAmount = TPCCTools.randomNumber(100, 500000)/100.0;


                paymentTransaction(cacheWrapper, terminalWarehouseID, customerWarehouseID, paymentAmount, districtID, customerDistrictID, customerID, customerLastName, customerByName);
                break;

            case ORDER_STATUS:
            	// clause 2.6.1.2
                districtID = TPCCTools.randomNumber(1, 10);

                y = TPCCTools.randomNumber(1, 100);
                customerLastName = null;
                customerID = -1;
                if(y <= 60)
                {
                	// clause 2.6.1.2 (dot 1)
                    customerByName = true;
                    customerLastName = lastName((int)TPCCTools.nonUniformRandom(this.c_run, TPCCTools.A_C_LAST, 0, TPCCTools.MAX_C_LAST));
                }
                else
                {
                	// clause 2.6.1.2 (dot 2)
                    customerByName = false;
                    customerID = TPCCTools.nonUniformRandom(this.c_id, TPCCTools.A_C_ID, 1, TPCCTools.NB_MAX_CUSTOMER);
                }

                orderStatusTransaction(cacheWrapper, terminalWarehouseID, districtID, customerID, customerLastName, customerByName);
                break;


            default:
                break;
        }


    }

    private String lastName(int num)
    {
        return nameTokens[num/100] + nameTokens[(num/10)%10] + nameTokens[num%10];
    }

    private void paymentTransaction(CacheWrapper cacheWrapper, long w_id, long c_w_id, double h_amount, long d_id, long c_d_id, long c_id, String c_last, boolean c_by_name)  throws Throwable
    {
        String w_name;
        String d_name;
        long namecnt;

        String new_c_last;

        String c_data = null, c_new_data, h_data;


        Warehouse w=new Warehouse();
        w.setW_id(w_id);

        boolean found=w.load(cacheWrapper);
        if(!found) throw new ElementNotFoundException("W_ID=" + w_id + " not found!");
        w.setW_ytd(h_amount);
        w.store(cacheWrapper);


        District d=new District();
        d.setD_id(d_id);
        d.setD_w_id(w_id);
        found=d.load(cacheWrapper);
        if(!found) throw new ElementNotFoundException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");

        d.setD_ytd(h_amount);
        d.store(cacheWrapper);



        Customer c=null;

        if(c_by_name)
        {
            new_c_last=c_last;
            List cList=null;
            cList=CustomerDAC.loadByCLast(cacheWrapper, c_w_id, c_d_id, new_c_last);
            while(cList==null || cList.isEmpty()){

                 new_c_last=lastName((int)TPCCTools.nonUniformRandom(this.c_run, TPCCTools.A_C_LAST, 0, TPCCTools.MAX_C_LAST));
                 cList=CustomerDAC.loadByCLast(cacheWrapper, c_w_id, c_d_id, new_c_last);

                //throw new ElementNotFoundException("C_LAST=" + c_last + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
            }

            Collections.sort(cList);


            namecnt=cList.size();



            if(namecnt%2 == 1) namecnt++;
            Iterator<Customer> itr=cList.iterator();

            for(int i = 1; i <= namecnt / 2; i++){

                c=itr.next();

            }

        }
        else
        {

            c=new Customer();
            c.setC_id(c_id);
            c.setC_d_id(c_d_id);
            c.setC_w_id(c_w_id);
            found=c.load(cacheWrapper);
            if(!found) throw new ElementNotFoundException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");


        }


        c.setC_balance(c.getC_balance()+h_amount);
        if(c.getC_credit().equals("BC"))
        {

            c_data=c.getC_data();

            c_new_data = c.getC_id() + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id  + " " + h_amount + " |";
            if(c_data.length() > c_new_data.length())
            {
                c_new_data += c_data.substring(0, c_data.length()-c_new_data.length());
            }
            else
            {
                c_new_data += c_data;
            }

            if(c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);

            c.setC_data(c_new_data);

            c.store(cacheWrapper);


        }
        else
        {
            c.store(cacheWrapper);

        }

        w_name=w.getW_name();
        d_name=d.getD_name();

        if(w_name.length() > 10) w_name = w_name.substring(0, 10);
        if(d_name.length() > 10) d_name = d_name.substring(0, 10);
        h_data = w_name + "    " + d_name;

        History h=new History(c.getC_id(), c_d_id, c_w_id, d_id, w_id, new Date(), h_amount, h_data);
        h.store(cacheWrapper);



    }

    private void newOrderTransaction(CacheWrapper cacheWrapper, long w_id, long d_id, long c_id, int o_ol_cnt, int o_all_local, long[] itemIDs, long[] supplierWarehouseIDs, long[] orderQuantities) throws Throwable
    {
        double c_discount, w_tax, d_tax = 0, i_price;
        long d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name, i_data, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        double[] itemPrices = new double[o_ol_cnt];
        double[] orderLineAmounts = new double[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        long[] stockQuantities = new long[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        long ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        double ol_amount, total_amount = 0;
        boolean newOrderRowInserted;



        Customer c=new Customer();
        Warehouse w=new Warehouse();

        c.setC_id(c_id);
        c.setC_d_id(d_id);
        c.setC_w_id(w_id);

        boolean found=c.load(cacheWrapper);

        if(!found) throw new ElementNotFoundException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");

        w.setW_id(w_id);

        found=w.load(cacheWrapper);
        if(!found) throw new ElementNotFoundException("W_ID=" + w_id + " not found!");




        //int j=0;
        //newOrderRowInserted = false;
        District d=new District();
        // see clause 2.4.2.2 (dot 4)
        // j++ < 10 ????? (� voir, mais pas necessaire)
        //while(!newOrderRowInserted && j++ < 10)  {


            d.setD_id(d_id);
            d.setD_w_id(w_id);
            found=d.load(cacheWrapper);
            if(!found) throw new ElementNotFoundException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");



            o_id=d.getD_next_o_id();


            NewOrder no=new NewOrder(o_id, d_id, w_id);

            no.store(cacheWrapper);

            //if(!newOrderRowInserted){
              //  log.info("The row was already on table new_order. Restarting...");
            //}


        //}

        d.setD_next_o_id(d.getD_next_o_id()+1);

        d.store(cacheWrapper);



        Order o=new Order(o_id, d_id, w_id, c_id, new Date(), -1, o_ol_cnt, o_all_local);

        o.store(cacheWrapper);



        // see clause 2.4.2.2 (dot 8)
        for(int ol_number = 1; ol_number <= o_ol_cnt; ol_number++)
        {
            ol_supply_w_id = supplierWarehouseIDs[ol_number-1];
            ol_i_id = itemIDs[ol_number-1];
            ol_quantity = orderQuantities[ol_number-1];

            // clause 2.4.2.2 (dot 8.1)
            Item i=new Item();
            i.setI_id(ol_i_id);
            found=i.load(cacheWrapper);
            if(!found)  throw new ElementNotFoundException("I_ID=" + ol_i_id + " not found!");



            itemPrices[ol_number-1] = i.getI_price();
            itemNames[ol_number-1] = i.getI_name();
            // clause 2.4.2.2 (dot 8.2)

            Stock s=new Stock();
            s.setS_i_id(ol_i_id);
            s.setS_w_id(ol_supply_w_id);
            found=s.load(cacheWrapper);
            if(!found) throw new ElementNotFoundException("I_ID=" + ol_i_id + " not found!");


            s_quantity= s.getS_quantity();
            stockQuantities[ol_number-1] = s_quantity;
            // clause 2.4.2.2 (dot 8.2)
            if(s_quantity - ol_quantity >= 10)
            {
                s_quantity -= ol_quantity;
            }
            else
            {
                s_quantity += -ol_quantity + 91;
            }

            if(ol_supply_w_id == w_id)
            {
                s_remote_cnt_increment = 0;
            }
            else
            {
                s_remote_cnt_increment = 1;
            }
            // clause 2.4.2.2 (dot 8.2)
            s.setS_quantity(s_quantity);
            s.setS_ytd(s.getS_ytd()+ol_quantity);
            s.setS_remote_cnt(s.getS_remote_cnt()+s_remote_cnt_increment);
            s.setS_order_cnt(s.getS_order_cnt()+1);
            s.store(cacheWrapper);


            // clause 2.4.2.2 (dot 8.3)
            ol_amount = ol_quantity * i.getI_price();
            orderLineAmounts[ol_number-1] = ol_amount;
            total_amount += ol_amount;
            // clause 2.4.2.2 (dot 8.4)
            i_data=i.getI_data();
            s_data=s.getS_data();
            if(i_data.indexOf(TPCCTools.ORIGINAL) != -1 && s_data.indexOf(TPCCTools.ORIGINAL) != -1)
            {
                brandGeneric[ol_number-1] = 'B';
            }
            else
            {
                brandGeneric[ol_number-1] = 'G';
            }

            switch((int)d_id)
            {
                case 1: ol_dist_info = s.getS_dist_01(); break;
                case 2: ol_dist_info = s.getS_dist_02(); break;
                case 3: ol_dist_info = s.getS_dist_03(); break;
                case 4: ol_dist_info = s.getS_dist_04(); break;
                case 5: ol_dist_info = s.getS_dist_05(); break;
                case 6: ol_dist_info = s.getS_dist_06(); break;
                case 7: ol_dist_info = s.getS_dist_07(); break;
                case 8: ol_dist_info = s.getS_dist_08(); break;
                case 9: ol_dist_info = s.getS_dist_09(); break;
                case 10: ol_dist_info = s.getS_dist_10(); break;
            }
            // clause 2.4.2.2 (dot 8.5)

            OrderLine ol=new OrderLine(o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, null, ol_quantity, ol_amount, ol_dist_info);
            ol.store(cacheWrapper);

        }

    }

    private void orderStatusTransaction(CacheWrapper cacheWrapper, long w_id, long d_id, long c_id, String c_last, boolean c_by_name) throws Throwable{
        long namecnt;

        boolean found=false;
        Customer c=null;
        if(c_by_name)
        {
            List<Customer> cList=CustomerDAC.loadByCLast(cacheWrapper, w_id, d_id, c_last);
            if(cList==null || cList.isEmpty()) throw new ElementNotFoundException("C_LAST=" + c_last + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");
            Collections.sort(cList);


            namecnt=cList.size();



            if(namecnt%2 == 1) namecnt++;
            Iterator<Customer> itr=cList.iterator();

            for(int i = 1; i <= namecnt / 2; i++){

                c=itr.next();

            }

        }
        else
        {
            // clause 2.6.2.2 (dot 3, Case 1)
            c=new Customer();
            c.setC_id(c_id);
            c.setC_d_id(d_id);
            c.setC_w_id(w_id);
            found=c.load(cacheWrapper);
            if(!found) throw new ElementNotFoundException("C_ID=" + c_id + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");

        }

        // clause 2.6.2.2 (dot 4)
        Order o=OrderDAC.loadByGreatestId(cacheWrapper, w_id, d_id, c_id);

        // clause 2.6.2.2 (dot 5)
        List<OrderLine> o_lines=OrderLineDAC.loadByOrder(cacheWrapper, o);


    }



}
