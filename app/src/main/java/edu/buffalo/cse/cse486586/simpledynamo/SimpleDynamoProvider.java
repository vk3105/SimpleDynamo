package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * REFERENCES : http://stackoverflow.com/questions/5697736/can-multiple-threads-wait-on-one-object-at-once
 */

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = SimpleDynamoProvider.class.getName();
    private Context mContext;


    // For Databases
    private SQLiteDatabase mSqLiteDatabase;
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_NAME = "SDP";
    private static final String KEY_COLUMN_NAME = "key";
    private static final String VALUE_COLUMN_NAME = "value";
    private static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    private static final String ORIGINATOR_PORT_COLUMN_NAME = "originatorPort";
    private static final String TABLE_NAME = "KeyValueTable";
    private static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + "( " +
            KEY_COLUMN_NAME + " TEXT PRIMARY KEY NOT NULL, " +
            TIMESTAMP_COLUMN_NAME + " TEXT NOT NULL, " +
            ORIGINATOR_PORT_COLUMN_NAME + " TEXT NOT NULL, " +
            VALUE_COLUMN_NAME + " TEXT NOT NULL);";


    // SERVER CLIENT FIELDS START
    private static final int SERVER_PORT = 10000;
    private static final int[] REMOTE_PORTS = {11108, 11112, 11116, 11120, 11124};
    private int MY_PORT;

    // Maintains the nodes order in the ring
    private static final int RING_SIZE = 5;
    private ArrayList<RingNode> nodeList = new ArrayList<RingNode>();
    private RingNode mNode = new RingNode();

    // An object to wait on in case of query
    private CustomMessage waitObj = createNewCustomMessage(Constants.QUERY);
    private CustomMessage recoveryWaitObj = createNewCustomMessage(Constants.RECOVERYRESULT);

    // Latches for the recieving all the query result
    private CountDownLatch allqueryLatch;
    private CountDownLatch singleQueryLatch;
    private CountDownLatch recoveryLatch = new CountDownLatch(4);
    private CountDownLatch deleteLatch;

    // SERVER CLIENT FIELDS ENDS


    /**
     * A class for handling Sqlite Database
     */
    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

        public MainDatabaseHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public SQLiteDatabase getWritableDatabase() {
            return super.getWritableDatabase();
        }

        @Override
        public SQLiteDatabase getReadableDatabase() {
            return super.getReadableDatabase();
        }

        @Override
        public synchronized void close() {
            super.close();
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }

        @Override
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            super.onDowngrade(db, oldVersion, newVersion);
        }

        @Override
        public void onOpen(SQLiteDatabase db) {
            super.onOpen(db);
        }
    }

    // OVERRIDDEN FUNCTIONS START

    // Synchronize the method
    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        callDelete(selection);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    // Synchronize the method
    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        //Timestamp for marking the order of insertion.
        Long timeStamp = System.currentTimeMillis();
        CustomMessage message = createNewCustomMessage(Constants.INSERT);
        message.setKey(values.getAsString(KEY_COLUMN_NAME));
        message.setValue(values.getAsString(VALUE_COLUMN_NAME));
        message.setTimeStamp(timeStamp.toString());

        callInsert(message);
        return uri;
    }

    // Synchronize the method
    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
        CustomMessage newCustomMessage = createNewCustomMessage(Constants.QUERY);
        newCustomMessage.setKey(selection);
        return callQuery(newCustomMessage);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }

    @Override
    public boolean onCreate() {
        // This is important to step.
        // Make sure to set a variable which can tell which node restarted.
        // This should be the first step for all the processes.
        boolean isRecoveryMode = false;
        SharedPreferences failCheck = this.getContext().getSharedPreferences("recovery", 0);
        if (failCheck.getBoolean("birth", true)) {
            Log.d(TAG, "BIRTH OF A MONSTER");
            failCheck.edit().putBoolean("birth", false).commit();
        } else {
            Log.d(TAG, "HOMECOMING");
            isRecoveryMode = true;
        }
        // Make sure you create the database first and initailize the server only in onCreate method.
        mContext = getContext();
        boolean isDatabaseCreated;
        isDatabaseCreated = createDataBase();
        initializeServer();

        // If the recovery needs to be done or not.
        if (isRecoveryMode) {
            new Thread(new RecoveryTask()).start();
        }

        return isDatabaseCreated;
    }

    // OVERRIDDEN FUNCTIONS ENDS

    // ****************************** HELPER FUNCTIONS START ***************************************

    /**
     * A simple thread which will help in recovery process.
     * <p>
     * We need the data from the 2 successors inorder to complete the recovery for its own messages.
     * We also need the data of the backups from the 2 predecessors inorder to complete the backups.
     */
    class RecoveryTask implements Runnable {
        @Override
        public void run() {

            recoveryWaitObj = createNewCustomMessage(Constants.RECOVERY);

            CustomMessage recoveryMessage = createNewCustomMessage(Constants.RECOVERY);
            sendMessage(recoveryMessage);

            recoveryLatch = new CountDownLatch(4);

            try {
                recoveryLatch.await();
            } catch (Exception e) {
                Log.e(TAG, e.getMessage());
                e.printStackTrace();
            }

            ConcurrentHashMap<String, String[]> finalKeyValMap = recoveryWaitObj.getKeyValMap();

            if (finalKeyValMap != null && finalKeyValMap.size() != 0) {
                for (String mapKey : finalKeyValMap.keySet()) {

                    String values[] = finalKeyValMap.get(mapKey);

                    int port = Integer.valueOf(values[1]);

                    if (port == mNode.getMyPort() ||
                            port == mNode.getPredPort() ||
                            port == mNode.getMySecondPredPort()) {
                        callSingleInsert(mapKey, values[0],
                                values[2], values[1]);
                    }
                }
            }
        }

    }

    /**
     * A function which creates the database.
     *
     * @return a boolean value if the database is created or not.
     */
    private boolean createDataBase() {
        MainDatabaseHelper databaseHelper = new MainDatabaseHelper(mContext);

        mSqLiteDatabase = databaseHelper.getWritableDatabase();
        if (mSqLiteDatabase == null) {
            Log.e(TAG, "Function : onCreate, database is null.!!! Have Fun Fixing it now :P");
            return false;
        } else {
            return true;
        }
    }

    /**
     * A function which initializes the server thread for communications.
     */
    private void initializeServer() {

        TelephonyManager tel = (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        MY_PORT = Integer.parseInt(myPort);
        sortAndSet();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "BAZOOKAAAA : " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * A function to send the message
     *
     * @param customMessage
     */
    private void sendMessage(CustomMessage customMessage) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, customMessage);
    }

    /**
     * Generates an empty message object with the specified type
     *
     * @param messageType : Type of message
     * @return An empty object.
     */
    private CustomMessage createNewCustomMessage(String messageType) {
        CustomMessage customMessage = new CustomMessage(messageType, MY_PORT,
                MY_PORT, null, null, null, null);
        return customMessage;
    }

    /**
     * A function to handle delete calls for a particular key
     *
     * @param key : A string value which needs to be deleted
     */
    private void callDelete(String key) {
        //@TODO Optimization needed. Sending messages all over :(
        String newSelection = KEY_COLUMN_NAME + " = ?";
        String[] newSelectionArgs = {key};

        CustomMessage customMessage = createNewCustomMessage(Constants.DELETE);
        customMessage.setKey(key);
        sendMessage(customMessage);
        deleteLatch = new CountDownLatch(5);

        try {
            deleteLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        mSqLiteDatabase.delete(TABLE_NAME, newSelection, newSelectionArgs);
    }

    /**
     * A function which helps in deleting a single key
     *
     * @param message : An object containg the key which needs to be deleted
     */
    private void callSingleDelete(CustomMessage message) {
        String newSelection = KEY_COLUMN_NAME + " = ?";
        String[] newSelectionArgs = {message.getKey()};

        int value = mSqLiteDatabase.delete(TABLE_NAME, newSelection, newSelectionArgs);
    }

    /**
     * A function to handle all types of queries
     *
     * @param customMessage
     * @return a cursor
     */
    private Cursor callQuery(CustomMessage customMessage) {
        if (customMessage.getKey().equals(Constants.QUERYLOCALALL)) {
            return callLocalAllQuery();
        } else if (customMessage.getKey().equals(Constants.QUERYGLOBALALL)) {
            return callGlobalAllQuery(customMessage);
        } else {
            return callSingleQuery(customMessage);
        }
    }

    /**
     * A function which helps in querying a single key from this node.
     *
     * @param customMessage Object containg the key
     * @return returns the value corresponding to the particular key.
     * <p>
     * NOTE : Its possible that the key should be at this node but still the node returns null.
     * This case happens when the current node is in recoverying process and the key value pair is not yet inserted.
     */
    private String getSingleQueryResult(CustomMessage customMessage) {
        String newSelection = KEY_COLUMN_NAME + " = ?";
        String[] newSelectionArgs = {customMessage.getKey()};

        Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null, newSelection, newSelectionArgs, null, null, null);
        cursor.moveToFirst();
        if (cursor != null && cursor.getCount() > 0) {
            return cursor.getString(cursor.getColumnIndex(VALUE_COLUMN_NAME));
        } else {
            return Constants.NULLVALUE;
        }
    }

    /**
     * A function which helps in querying a single key.
     *
     * @param customMessage An object
     * @return A cursor containing the value of the key
     */
    private Cursor callSingleQuery(CustomMessage customMessage) {

        //Key belong to this node
        RingNode node = findPositionOfKey(customMessage.getKey());

        if (node.getMyPort() == MY_PORT) {
            String newSelection = KEY_COLUMN_NAME + " = ?";
            String[] newSelectionArgs = {customMessage.getKey()};

            Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null, newSelection, newSelectionArgs, null, null, null);
            return cursor;
        } else {
            customMessage.setToPort(node.getMyPort());
            sendMessage(customMessage);

            waitObj = createNewCustomMessage(Constants.QUERYGLOBALRESULT);

            // We will query the original node which should contain the key value pair
            singleQueryLatch = new CountDownLatch(1);
            try {
                singleQueryLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // If the result is null from the above query we will send the query to the successor
            if (waitObj.getValue().equals(Constants.NULLVALUE)) {
                customMessage.setToPort(node.getMySuccPort());
                sendMessage(customMessage);

                waitObj = createNewCustomMessage(Constants.QUERYGLOBALRESULT);

                singleQueryLatch = new CountDownLatch(1);

                try {
                    singleQueryLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // If the result is null from the above query too we will send the query to the second successor
                if (waitObj.getValue().equals(Constants.NULLVALUE)) {
                    customMessage.setToPort(node.getMySecondSuccPort());
                    sendMessage(customMessage);

                    waitObj = createNewCustomMessage(Constants.QUERYGLOBALRESULT);
                    singleQueryLatch = new CountDownLatch(1);

                    try {
                        singleQueryLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // we build a matrix cursor from the returned values to return values
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME, VALUE_COLUMN_NAME});
            matrixCursor.addRow(new Object[]{waitObj.getKey(), waitObj.getValue()});
            return matrixCursor;
        }
    }

    /**
     * A function which retrieves all the key value pairs from this node.
     *
     * @return a cursor
     */
    private Cursor callLocalAllQuery() {
        Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null,
                null, null, null, null, null);

        cursor.moveToFirst();

        if (cursor != null && cursor.getCount() > 0) {
            String key = cursor.getString(cursor.getColumnIndex(KEY_COLUMN_NAME));
            String keyValue = cursor.getString(cursor.getColumnIndex(VALUE_COLUMN_NAME));

            Log.d(TAG, key + " : " + keyValue);
        }
        return cursor;
    }

    /**
     * A function which collects all the key value pairs and returns them
     *
     * @param customMessage
     * @return a cursor
     */
    private Cursor callGlobalAllQuery(CustomMessage customMessage) {

        customMessage.setMessageType(Constants.QUERYGLOBAL);
        sendMessage(customMessage);

        waitObj = createNewCustomMessage(Constants.QUERYGLOBALRESULT);

        allqueryLatch = new CountDownLatch(4);

        try {
            allqueryLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ConcurrentHashMap<String, String[]> localKeyValMap = getAllLocalKeyValuePairs();

        MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME, VALUE_COLUMN_NAME});

        // collect all the values and pack them in matrix cursor
        ConcurrentHashMap<String, String[]> finalKeyValMap = waitObj.getKeyValMap();
        finalKeyValMap.putAll(localKeyValMap);

        if (finalKeyValMap != null && finalKeyValMap.size() != 0) {
            for (String mapKey : finalKeyValMap.keySet()) {
                String value[] = finalKeyValMap.get(mapKey);
                matrixCursor.addRow(new Object[]{mapKey, value[0]});
            }
        }
        return matrixCursor;
    }

    /**
     * A function to handle all the insert calls
     *
     * @param customMessage An object with the given key value pair
     */
    private void callInsert(CustomMessage customMessage) {

        RingNode node = findPositionOfKey(customMessage.getKey());

        if (node.getMyPort() == MY_PORT) {

            Log.d(TAG, "INSERTING 1ST CONDITION: " + MY_PORT);

            callSingleInsert(customMessage.getKey(), customMessage.getValue(),
                    customMessage.getTimeStamp(), String.valueOf(MY_PORT));

            customMessage.setSenderPort(MY_PORT);
            customMessage.setMessageType(Constants.INSERTFINAL);
            sendMessage(customMessage);

        } else {
            Log.d(TAG, "INSERTING 2ND CONDITION: " + node.getMyPort());
            customMessage.setToPort(node.getMyPort());
            sendMessage(customMessage);
        }
    }

    /**
     * A function to insert a single key value pair on the node
     *
     * @param key             : Key to be inserted
     * @param value           : Value to be inserted
     * @param timeStamp       : Timestamp of the key to be inserted
     * @param originatingPort : Port to which the key actually belongs
     */
    private void callSingleInsert(String key, String value, String timeStamp, String originatingPort) {

        String newSelection = KEY_COLUMN_NAME + " = ?";
        String[] newSelectionArgs = {key};

        Cursor cursor = mSqLiteDatabase.query(TABLE_NAME, null, newSelection, newSelectionArgs, null, null, null);
        cursor.moveToFirst();

        ContentValues values = new ContentValues();
        values.put(KEY_COLUMN_NAME, key);
        values.put(VALUE_COLUMN_NAME, value);
        values.put(TIMESTAMP_COLUMN_NAME, timeStamp);
        values.put(ORIGINATOR_PORT_COLUMN_NAME, originatingPort);

        if (cursor != null && cursor.getCount() > 0) {
            String existingTimeStamp = cursor.getString(cursor.getColumnIndex(TIMESTAMP_COLUMN_NAME));
            if (timeStamp.compareTo(existingTimeStamp) > 0) {
                mSqLiteDatabase.replace(TABLE_NAME, "", values);
            }
        } else {
            mSqLiteDatabase.replace(TABLE_NAME, "", values);
        }

    }

    /**
     * A function which retrieves all the key value pairs from this node.
     *
     * @return A hash map containing all the key value pair
     */
    private ConcurrentHashMap<String, String[]> getAllLocalKeyValuePairs() {

        Cursor localDumpCursor = mSqLiteDatabase.query(TABLE_NAME, null,
                null, null, null, null, null);

        ConcurrentHashMap<String, String[]> localKeyValMap = new ConcurrentHashMap<String, String[]>();

        if (localDumpCursor != null && localDumpCursor.getCount() > 0) {
            localDumpCursor.moveToFirst();
            while (localDumpCursor.isAfterLast() == false) {
                String key = localDumpCursor.getString(localDumpCursor.getColumnIndex(KEY_COLUMN_NAME));
                String keyValue = localDumpCursor.getString(localDumpCursor.getColumnIndex(VALUE_COLUMN_NAME));
                String port = localDumpCursor.getString(localDumpCursor.getColumnIndex(ORIGINATOR_PORT_COLUMN_NAME));
                String timeStamp = localDumpCursor.getString(localDumpCursor.getColumnIndex(TIMESTAMP_COLUMN_NAME));
                String value[] = {keyValue, port, timeStamp};
                localKeyValMap.put(key, value);
                localDumpCursor.moveToNext();
            }
        }

        return localKeyValMap;
    }

    /**
     * A function which tells where the key should be stored in the ring
     *
     * @param key : A string value of the encrypted key
     * @return Node to which the key belongs
     */
    private RingNode findPositionOfKey(String key) {
        String hashedKey = genHash(key);

        RingNode ringNode = new RingNode();

        if ((hashedKey.compareTo(nodeList.get(RING_SIZE - 1).getMyKeyHash()) > 0) ||
                (hashedKey.compareTo(nodeList.get(0).getMyKeyHash()) < 0)) {
            ringNode = nodeList.get(0);
        } else {
            for (int i = 0; i < RING_SIZE - 1; i++) {
                if ((hashedKey.compareTo(nodeList.get(i).getMyKeyHash()) > 0)
                        && (hashedKey.compareTo(nodeList.get(i + 1).getMyKeyHash()) < 0)) {
                    ringNode = nodeList.get(i + 1);
                    break;
                }
            }

        }
        return ringNode;
    }

    /**
     * A SHA-1 encryption code for keys and nodes
     *
     * @param input : A string
     * @return A string
     */
    private String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * A function which helps in assigning the successors and predecessors of all the node and helps in forming the ring
     */
    private void sortAndSet() {
        for (int i = 0; i < REMOTE_PORTS.length; i++) {
            String key = genHash(String.valueOf(REMOTE_PORTS[i] / 2));
            int value = REMOTE_PORTS[i];
            RingNode node = new RingNode(value, value, value, value, value, key, key, key);
            nodeList.add(node);
        }

        Collections.sort(nodeList);
        int N = nodeList.size();
        for (int i = 0; i < N; i++) {
            RingNode node = nodeList.get(i);

            node.setPredPort(nodeList.get((N + i - 1) % N).getMyPort());
            node.setSuccPort(nodeList.get((N + i + 1) % N).getMyPort());
            node.setMyPredKeyHash(nodeList.get((N + i - 1) % N).getMyKeyHash());
            node.setMySecondPredPort(nodeList.get((N + i - 2) % N).getMyPort());
            node.setMySuccKeyHash(nodeList.get((N + i + 1) % N).getMyKeyHash());
            node.setMySecondSuccPort(nodeList.get((N + i + 2) % N).getMyPort());
            nodeList.set(i, node);

            if (node.getMyPort() == MY_PORT) {
                mNode = node;
            }
        }
    }

    /**
     * A function which decomposes the given string and makes an object of CustomMessage
     *
     * @param message A message string
     * @return An object of CustomMessage
     */
    private CustomMessage decomposeMessage(String message) {

        String messageArr[] = message.split(Constants.DELIM);

        String messageType = messageArr[0].trim();
        int senderPort = Integer.parseInt(messageArr[1].trim());
        int toPort = Integer.parseInt(messageArr[2].trim());
        String key = messageArr[3].trim();
        String value = messageArr[4].trim();
        String timeStamp = messageArr[5].trim();
        String hashData = messageArr[6].trim();

        ConcurrentHashMap<String, String[]> keyValMap = null;

        if (!hashData.equals(Constants.NULLVALUE) && hashData.length() != 0) {

            keyValMap = new ConcurrentHashMap<String, String[]>();
            String hashDataArr[] = hashData.split(Constants.DATASEPDELIM);
            int len = hashDataArr.length;
            for (int i = 0; i < len; i++) {
                String hashDataSepArr[] = hashDataArr[i].split(Constants.DATADELIM);
                String hashKey = hashDataSepArr[0];
                String hashValue[] = hashDataSepArr[1].split(",");
                keyValMap.put(hashKey, hashValue);
            }
        }

        CustomMessage composeMessage = new CustomMessage(messageType, senderPort, toPort, key, value,
                timeStamp, keyValMap);

        return composeMessage;
    }


    // ****************************** HELPER FUNCTIONS ENDS ****************************************


    /**
     * Server for all the communications
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {

                while (true) {

                    Socket clientSocket = serverSocket.accept();

                    OutputStream serverSocketOutputStream = clientSocket.getOutputStream();
                    InputStreamReader serverSocketInputStreamReader = new InputStreamReader(clientSocket.getInputStream());

                    PrintWriter serverOutputPrintWriter = new PrintWriter(serverSocketOutputStream, true);
                    BufferedReader serverInputBufferedReader = new BufferedReader(serverSocketInputStreamReader);

                    String commMessage;
                    String clientMessage = "";

                    while ((commMessage = serverInputBufferedReader.readLine()) != null) {
                        if (commMessage.equals(Constants.SYN)) {
                            serverOutputPrintWriter.println(Constants.SYNACK);
                        } else if (commMessage.equals(Constants.ACK)) {
                            serverOutputPrintWriter.println(Constants.ACK);
                        } else if (commMessage.equals(Constants.STOP)) {
                            serverOutputPrintWriter.println(Constants.STOPPED);
                            break;
                        } else {
                            if (commMessage.length() != 0) {
                                clientMessage = commMessage;
                                serverOutputPrintWriter.println(Constants.OK);
                            }
                        }
                    }
                    serverSocketOutputStream.close();
                    serverInputBufferedReader.close();
                    serverOutputPrintWriter.close();
                    serverSocketInputStreamReader.close();

                    if (null == clientMessage || clientMessage.trim().length() == 0) {
                        return null;
                    }

                    CustomMessage message = decomposeMessage(clientMessage);

                    if (message.getMessageType().equals(Constants.INSERTFINAL)) {

                        RingNode originalNode = findPositionOfKey(message.getKey());

                        callSingleInsert(message.getKey(), message.getValue(),
                                message.getTimeStamp(), String.valueOf(originalNode.getMyPort()));

                    } else if (message.getMessageType().equals(Constants.INSERT)) {

                        callInsert(message);

                    } else if (message.getMessageType().equals(Constants.QUERY)) {

                        String value = getSingleQueryResult(message);
                        message.setMessageType(Constants.QUERYRESULT);
                        message.setToPort(message.getSenderPort());
                        message.setValue(value);
                        sendMessage(message);

                    } else if (message.getMessageType().equals(Constants.QUERYRESULT)) {

                        waitObj.setKey(message.getKey());
                        waitObj.setValue(message.getValue());
                        singleQueryLatch.countDown();

                    } else if (message.getMessageType().equals(Constants.QUERYGLOBAL)) {

                        ConcurrentHashMap<String, String[]> localMap = getAllLocalKeyValuePairs();
                        message.setToPort(message.getSenderPort());
                        message.setMessageType(Constants.QUERYGLOBALRESULT);
                        message.setKeyValMap(localMap);
                        sendMessage(message);

                    } else if (message.getMessageType().equals(Constants.QUERYGLOBALRESULT)) {

                        ConcurrentHashMap<String, String[]> localMap = waitObj.getKeyValMap();
                        if (localMap == null) {
                            localMap = new ConcurrentHashMap<String, String[]>();
                        }
                        localMap.putAll(message.getKeyValMap());
                        waitObj.setKeyValMap(localMap);
                        allqueryLatch.countDown();

                    } else if (message.getMessageType().equals(Constants.DELETE)) {

                        callSingleDelete(message);
                        message.setMessageType(Constants.DELETERESULT);
                        message.setToPort(message.getSenderPort());
                        sendMessage(message);

                    } else if (message.getMessageType().equals(Constants.DELETERESULT)) {

                        deleteLatch.countDown();

                    } else if (message.getMessageType().equals(Constants.RECOVERY)) {

                        ConcurrentHashMap<String, String[]> localMap = getAllLocalKeyValuePairs();
                        message.setToPort(message.getSenderPort());
                        message.setSenderPort(MY_PORT);
                        message.setMessageType(Constants.RECOVERYRESULT);
                        message.setKeyValMap(localMap);

                        sendMessage(message);

                    } else if (message.getMessageType().equals(Constants.RECOVERYRESULT)) {

                        ConcurrentHashMap<String, String[]> localMap = recoveryWaitObj.getKeyValMap();

                        if (localMap == null) {
                            localMap = new ConcurrentHashMap<String, String[]>();
                        }
                        if (message.getKeyValMap() != null) {
                            localMap.putAll(message.getKeyValMap());
                            recoveryWaitObj.setKeyValMap(localMap);
                        }
                        recoveryLatch.countDown();
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException : " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "ServerTask socket Exception : " + e.getMessage());
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author vkumar25
     */

    /**
     * A simple message sending protocol
     *
     * @param message
     */
    private boolean sendMessageForClient(CustomMessage message) {
        boolean isSuccessful = false;
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.getToPort());

            OutputStream clientOutputStream = socket.getOutputStream();
            InputStreamReader clientInputStreamReader = new InputStreamReader(socket.getInputStream());

            PrintWriter clientOutputPrintWriter = new PrintWriter(clientOutputStream, true);
            BufferedReader clientInputBufferReader = new BufferedReader(clientInputStreamReader);

            String msgFromServer;

            clientOutputPrintWriter.println(Constants.SYN);

            while ((msgFromServer = clientInputBufferReader.readLine()) != null) {

                if (msgFromServer.equals(Constants.SYNACK)) {
                    clientOutputPrintWriter.println(Constants.ACK);
                } else if (msgFromServer.equals(Constants.ACK)) {
                    clientOutputPrintWriter.println(message.toString());
                } else if (msgFromServer.equals(Constants.OK)) {
                    clientOutputPrintWriter.println(Constants.STOP);
                } else if (msgFromServer.equals(Constants.STOPPED)) {
                    break;
                }
            }
            clientOutputPrintWriter.close();
            clientInputBufferReader.close();
            socket.close();
            if (msgFromServer != null && msgFromServer.equals(Constants.STOPPED)) {
                isSuccessful = true;
            }
        } catch (Exception e) {
            Log.e(TAG, "This is Client Sparta : " + e.getMessage());
            e.printStackTrace();
        }

        return isSuccessful;
    }


    /**
     * Client for sending data
     */
    private class ClientTask extends AsyncTask<CustomMessage, Void, Void> {

        @Override
        protected Void doInBackground(CustomMessage... msgs) {

            CustomMessage message = msgs[0];

            if (message.getMessageType().equals(Constants.QUERYRESULT) ||
                    message.getMessageType().equals(Constants.QUERYGLOBALRESULT) ||
                    message.getMessageType().equals(Constants.RECOVERYRESULT) ||
                    message.getMessageType().equals(Constants.DELETERESULT)) {

                sendMessageForClient(message);

            } else if (message.getMessageType().equals(Constants.DELETE)) {
                for (int i = 0; i < RING_SIZE; i++) {
                    message.setToPort(REMOTE_PORTS[i]);
                    if (!sendMessageForClient(message)) {
                        deleteLatch.countDown();
                    }
                }
            } else if (message.getMessageType().equals(Constants.QUERY)) {

                if (!sendMessageForClient(message)) {
                    RingNode node = findPositionOfKey(message.getKey());

                    message.setToPort(node.getSuccPort());
                    if (!sendMessageForClient(message)) {
                        message.setToPort(node.getMySecondSuccPort());
                        sendMessageForClient(message);
                    }
                }

            } else if (message.getMessageType().equals(Constants.INSERT)) {

                if (!sendMessageForClient(message)) {

                    RingNode node = findPositionOfKey(message.getKey());

                    message.setMessageType(Constants.INSERTFINAL);
                    message.setToPort(node.getSuccPort());
                    sendMessageForClient(message);

                    message.setToPort(node.getMySecondSuccPort());
                    sendMessageForClient(message);
                }

            } else if (message.getMessageType().equals(Constants.INSERTFINAL)) {
                message.setToPort(mNode.getSuccPort());
                sendMessageForClient(message);

                message.setToPort(mNode.getMySecondSuccPort());
                sendMessageForClient(message);


            } else if (message.getMessageType().equals(Constants.QUERYGLOBAL)) {
                for (int i = 0; i < RING_SIZE; i++) {
                    if (MY_PORT != REMOTE_PORTS[i]) {
                        message.setToPort(REMOTE_PORTS[i]);
                        if (!sendMessageForClient(message)) {
                            allqueryLatch.countDown();
                        }
                    }
                }
            } else if (message.getMessageType().equals(Constants.RECOVERY)) {
                for (int i = 0; i < RING_SIZE; i++) {
                    if (MY_PORT != REMOTE_PORTS[i]) {
                        message.setToPort(REMOTE_PORTS[i]);
                        sendMessageForClient(message);
                    }
                }
            }
            return null;
        }
    }
}
