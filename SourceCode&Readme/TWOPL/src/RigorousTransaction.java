

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class RigorousTransaction {
    public static int ts=0;
    public static Hashtable<String, String> tranOperationTable = new Hashtable<>();
    public static Hashtable<String, List<String>> tranItemsLocked = new Hashtable<>();
    public static Hashtable<String, List<String>> chainedOperations = new Hashtable<>();

    public static void startTransaction(String operation) {
        String tNo = String.valueOf(operation.charAt(1));
        int timeStamp = ++ts;
        String variable = "ACTIVE" + "," + timeStamp;
        tranOperationTable.put(tNo, variable);
        TwoPhaseLocking.write("TRANSACTION BEGINS, ID:"+tNo+" TS:"+timeStamp+" STATE=ACTIVE");

    }
    public static void rigorousTwoPhaseReadTransaction(String operation) {

        List<String> holdingTrans = new ArrayList<String>();
        List<String> itemsList = new ArrayList<String>();
        String tNo = String.valueOf(operation.charAt(1));
        String dataitem = String.valueOf(operation.charAt(2));
        String value = tranOperationTable.get(tNo);
        String state = value.split(",")[0];
        if (state.equals("ACTIVE")) {
            if (Lock.dataItemLock.get(dataitem) == null) {
                holdingTrans.add(tNo);
                if (tranItemsLocked.get(tNo) != null)
                    itemsList = tranItemsLocked.get(tNo);
                if (!itemsList.contains(dataitem))
                    itemsList.add(dataitem);
                tranItemsLocked.put(tNo, itemsList);
                Lock.dataItemLock.put(dataitem, "READ");
                TwoPhaseLocking.write( dataitem +" IS READ LOCKED BY "+ " T" + tNo);
                Lock.holdingTransactions.put(dataitem, holdingTrans);

            } else {
                holdingTrans = Lock.holdingTransactions.get(dataitem);
                if (Lock.dataItemLock.get(dataitem).equals("READ")) {
                    if (holdingTrans.contains(tNo)) {
                        TwoPhaseLocking.write(" READ DATAITEM " + dataitem);
                    } else {
                        List<String> trans = new ArrayList<String>();
                        trans = Lock.holdingTransactions.get(dataitem);
                        trans.add(tNo);
                        Lock.holdingTransactions.put(dataitem, trans);
                        if (tranItemsLocked.get(tNo) != null)
                            itemsList = tranItemsLocked.get(tNo);
                        if (!itemsList.contains(dataitem))
                            itemsList.add(dataitem);
                        TwoPhaseLocking.write("SHARED LOCK ON " + dataitem + " BY TRANSACTION " + trans.toString());
                        tranItemsLocked.put(tNo, itemsList);
                    }
                } else {
                    Lock.waitDieDecide(tNo, holdingTrans, operation);
                }
            }
        } else if (state.equals("ABORT")) {

            TwoPhaseLocking.write("TRANSACION ABORTED DUE TO WAIT-DIE");
        } else if (state.equals("BLOCKED")) {
            List<String> opList = new ArrayList<String>();
            opList.add(operation);
            chainedOperations.put(tNo, opList);
            TwoPhaseLocking.write("TRANSACTION STATE BLOCKED DUE TO WAIT-DIE ");
        }
    }

    public static void rigorousTwoPhaseWriteTransaction(String operation) {
        List<String> holdingTrans = new ArrayList<String>();
        List<String> itemsList = new ArrayList<String>();
        String tNo = String.valueOf(operation.charAt(1));
        String dataValue = String.valueOf(operation.charAt(2));
        String value = tranOperationTable.get(tNo);
        String state = value.split(",")[0];
        if (state.equals("ACTIVE")) {
            if (Lock.dataItemLock.get(dataValue) == null) {
                holdingTrans.add(tNo);
                if (tranItemsLocked != null)
                    itemsList = tranItemsLocked.get(tNo);
                if (!itemsList.contains(dataValue))
                    itemsList.add(dataValue);
                tranItemsLocked.put(tNo, itemsList);
                Lock.dataItemLock.put(dataValue, "WRITE");
                TwoPhaseLocking.write(" :: WRITE LOCK ON " + dataValue);
                Lock.holdingTransactions.put(dataValue, holdingTrans);
            } else {
                holdingTrans = Lock.holdingTransactions.get(dataValue);
                if (Lock.dataItemLock.get(dataValue).equals("READ")) {
                    if (holdingTrans.size() == 1) {
                        if (holdingTrans.get(0).equals(tNo)) {


                            Lock.dataItemLock.put(dataValue, "WRITE");
                            TwoPhaseLocking.write("READ LOCK ON "+ dataValue+ " IS UPGRADED TO WRITE LOCK");
                        }
                    } else {
                        Lock.waitDieDecide(tNo, holdingTrans, operation);
                    }
                } else {
                    if (holdingTrans.get(0).equals(tNo)) {

                    } else {
                        Lock.waitDieDecide(tNo, holdingTrans, operation);
                    }
                }
            }
        } else if (state.equals("BLOCKED")) {
            List<String> opList = new ArrayList<String>();
            opList.add(operation);
            chainedOperations.put(tNo, opList);

            TwoPhaseLocking.write("TRASACTION STATE BLOCKED DUE TO WAIT-DIE");
        } else if (state.equals("ABORT")) {

            TwoPhaseLocking.write("TRANSACTION ABORTED DUE TO WAIT-DIE");
        }
    }
    public static void closeTransaction(String operation) {
        List<String> releasedItems = new ArrayList<String>();
        String tNo = String.valueOf(operation.charAt(1));
        String whole = tranOperationTable.get(tNo);
        String state = whole.split(",")[0];
        releasedItems = tranItemsLocked.get(tNo);
        if (state.equals("ACTIVE")) {
            tranOperationTable.put(tNo, "COMMITTED" + "," + whole.split(",")[1]);
            Lock.freeLocks(releasedItems, tNo);

            TwoPhaseLocking.write("TRANSACTION T" + tNo + " IS COMMITTED");
        } else if (state.equals("BLOCKED")) {
            List<String> opList = new ArrayList<String>();
            opList = RigorousTransaction.chainedOperations.get(tNo);
            if (!chainedOperations.contains(operation))
                opList.add(operation);
            chainedOperations.put(tNo, opList);

            TwoPhaseLocking.write("TRANSACTION STATE BLOCKED AND THE OPERATION IS ADDED TO WAITING TRANSACTIONS");
        } else if (state.equals("ABORT")) {
            TwoPhaseLocking.write("TRANSACTION T"+tNo+" IS ABORTED");
        }
    }


}
