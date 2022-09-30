

import java.util.List;
import java.util.ArrayList;
import java.util.Hashtable;

public class Lock {
    public static Hashtable<String, String> dataItemLock = new Hashtable<>();
    public static Hashtable<String, List<String>> blockedTransactions = new Hashtable<>();
    public static Hashtable<String, List<String>> holdingTransactions = new Hashtable<>();

    public static void waitDieDecide(String key, List<String> holdTrans, String op) {
        String v1 = RigorousTransaction.tranOperationTable.get(key);
        String v2 = null;
        String TimeStamp1 = v1.split(",")[1];
        for (int i = 1; i <= holdTrans.size(); i++) {
            if (RigorousTransaction.tranOperationTable.get(key).split(",")[0].equals("ACTIVE")
                    || RigorousTransaction.tranOperationTable.get(key).split(",")[0].equals("BLOCKED")) {
                v2 = RigorousTransaction.tranOperationTable.get(holdTrans.get(i - 1));
                String TimeStamp2 = v2.split(",")[1];
                if (Integer.parseInt(TimeStamp1) <=Integer.parseInt(TimeStamp2)) {

                    waitTrans(key, op);
                } else {

                    abortTransaction(key, op);
                }
            }
        }

    }

    private static void abortTransaction(String key, String operation) {

        List<String> releasedItems = new ArrayList<String>();
        String TS = RigorousTransaction.tranOperationTable.get(key).split(",")[1];
        RigorousTransaction.tranOperationTable.put(key, "ABORT" + "," + TS);
        TwoPhaseLocking.write("TRANSACTION" + key + " IS ABORTED,CHECK AND RELEASE IF IT HOLDS ANY LOCKS");

        releasedItems = RigorousTransaction.tranItemsLocked.get(key);
        if (releasedItems != null)
            freeLocks(releasedItems, key);

    }
    public static void freeLocks(List<String> releasedItems, String key) {
        List<String> holdingTrans = new ArrayList<String>();
        RigorousTransaction.tranItemsLocked.remove(key);
        for (int i = 0; i < releasedItems.size(); i++) {
            if (dataItemLock.get(releasedItems.get(i)).equals("WRITE")) {
                String item = releasedItems.get(i);
                dataItemLock.remove(item);
                holdingTransactions.remove(item);

            } else if (dataItemLock.get(releasedItems.get(i)).equals("READ")) {
                String dataitem = releasedItems.get(i);
                if (holdingTransactions.get(dataitem).size() > 1) {
                    holdingTrans = holdingTransactions.get(dataitem);
                    holdingTrans.remove(holdingTrans.indexOf(key));
                    holdingTransactions.put(dataitem, holdingTrans);
                } else {
                    dataItemLock.remove(dataitem);
                    holdingTransactions.remove(dataitem);
                }
            }
        }
    }
    private static void waitTrans(String key, String operation) {
        String dataitem = String.valueOf(operation.charAt(2));
        List<String> waitingTans = new ArrayList<String>();
        List<String> queuedOperations = new ArrayList<String>();
        String TS = RigorousTransaction.tranOperationTable.get(key).split(",")[1];
        RigorousTransaction.tranOperationTable.put(key, "BLOCKED" + "," + TS);
        TwoPhaseLocking.write("TRANSACTION" + key + " IS BLOCKED");
        if (blockedTransactions.get(dataitem) != null)
            waitingTans = blockedTransactions.get(dataitem);
        if (!waitingTans.contains(key))
            waitingTans.add(key);
        blockedTransactions.put(dataitem, waitingTans);
        if (RigorousTransaction.chainedOperations.get(key) != null)
            queuedOperations = RigorousTransaction.chainedOperations.get(key);
        if (!queuedOperations.contains(operation)) {
            queuedOperations.add(operation);
            TwoPhaseLocking.write(" AND THE OPERATION IS ADDDED TO WAITING TRANSACTIONS");
        }

        RigorousTransaction.chainedOperations.put(key, queuedOperations);
    }









}
