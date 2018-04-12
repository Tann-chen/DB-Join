import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class Join {
    public static boolean DEBUG = true;
    public static int MAX_TUPLES;
    public static int MAX_BLOCKS;
    public static final float K = 1024.0f;
    public static int TUPLES_OF_BLOCK;
    public static int BYTES_OF_TUPLE;
    public static int BLOCK_SIZE;
    public static float RUNNING_MEMORY = 1024; // kb
    public static int iocost = 0;


    public static void main(String[] args) {
        System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
        long startTime = System.currentTimeMillis();

        //pass 1
        //sort T1
        initRunTimeParam(40, 101);
        int t1SubLists = phaseOne("JoinT1.txt", "t1");
        recordTime(startTime,"The time after sub-sorted T1");
        //sort T2
        initRunTimeParam(130, 28);
        int t2SubLists = phaseOne("JoinT2.txt", "t2");
        recordTime(startTime,"The time after sub-sorted T2");
        System.out.println("=====================================");
        System.out.println("Pass 1 IO:" + iocost);

        passTwo("t1",t1SubLists,"t2",t2SubLists);
        recordTime(startTime, "The time after join T1 and T2");
        System.out.println("Total IO:" + iocost);
    }

    /**
     * Init the runtime parameters for the sorting
     */
    public static void initRunTimeParam(int tuplesOfBlock, int bytesOfTuple) {
        TUPLES_OF_BLOCK = tuplesOfBlock;
        BYTES_OF_TUPLE = bytesOfTuple;
        BLOCK_SIZE = TUPLES_OF_BLOCK * BYTES_OF_TUPLE;
        float freeMemory = Runtime.getRuntime().freeMemory() / (K * K);
        if (freeMemory > 7) {
            RUNNING_MEMORY *= 3.4;
        } else {
            RUNNING_MEMORY *= 2;
        }
        MAX_BLOCKS = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNING_MEMORY * K) / BLOCK_SIZE);
        MAX_TUPLES = TUPLES_OF_BLOCK * MAX_BLOCKS;
    }

    /**
     * The input is T1 and T2 input file, the output is sorted file
     */
    public static int phaseOne(String inputFile, String outputPrefix) {
        float startMemory = Runtime.getRuntime().freeMemory() / K;
        long startTime = System.currentTimeMillis();
        int sublistCount = 0;
        int readLength = 0;
        int lines = 0;
        if (DEBUG) {
            System.out.println("**************************" + outputPrefix + " PHASE ONE **************************");
            System.out.println("Max tuples to fill:" + MAX_TUPLES);
        }

        byte[][] sublistbyte = new byte[MAX_TUPLES][BYTES_OF_TUPLE];
        byte[] blockBuffer = new byte[TUPLES_OF_BLOCK * BYTES_OF_TUPLE];
        if (DEBUG) {
            System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
        }
        try {
            FileInputStream ios = new FileInputStream(inputFile);
            while ((readLength = ios.read(blockBuffer)) != -1) {
                iocost++;
                for (int i = 0; i < readLength / BYTES_OF_TUPLE; i++) {
                    System.arraycopy(blockBuffer, BYTES_OF_TUPLE * i, sublistbyte[lines++], 0, BYTES_OF_TUPLE);
                }
                if (lines == MAX_TUPLES || readLength < blockBuffer.length) {
                    // sort the sublist
                    QuickSort.quicksort(sublistbyte, 0, lines - 1);
                    FileOutputStream out = new FileOutputStream(outputPrefix + "_" + sublistCount++ + ".txt");
                    int lineCount = 0;
                    while (lineCount < lines) {
                        System.arraycopy(sublistbyte[lineCount], 0, blockBuffer, BYTES_OF_TUPLE * (lineCount % TUPLES_OF_BLOCK), BYTES_OF_TUPLE);
                        lineCount++;
                        if (lineCount % TUPLES_OF_BLOCK == 0) {
                            out.write(blockBuffer, 0, BYTES_OF_TUPLE * TUPLES_OF_BLOCK);
                            iocost++;
                        } else if (lineCount == lines) {
                            out.write(blockBuffer, 0, BYTES_OF_TUPLE * (lineCount % TUPLES_OF_BLOCK));
                            iocost++;
                        }
                    }
                    lines = 0;
                    out.close();
                    out = null;
                    if (DEBUG) {
                        System.out.println("Finish " + sublistCount + " sublist, Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
                    }
                }
            }
            ios.close();
            ios = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (DEBUG) {
            recordTime(startTime, outputPrefix + " phase 1");
            recordMemory(startMemory, outputPrefix + " phase 1");
        }
        blockBuffer = null;
        sublistbyte = null;
        System.gc();
        return sublistCount;
    }
    
    
    

    public static void passTwo(String t1Prefix, int t1SublistsCount, String t2Prefix, int t2SublistsCount) {
        System.gc();
        byte[] outputBuffer = new byte[4048];
        byte[] outputBuffer2 = new byte[4048*2];
        int outputPointer = 0;
        int outputPointer2 = 0;
        byte[][][] t1Buffer = new byte[t1SublistsCount][40][101];   //one buffer for every t1 sublist
        byte[][][] t2Buffer = new byte[t2SublistsCount][130][28];   //one buffer for every t2 sublist

        try {
            FileOutputStream out = new FileOutputStream("GPA.txt");
            FileOutputStream out2 = new FileOutputStream("Join.txt");
            FileInputStream[] in1Arr = new FileInputStream[t1SublistsCount];
            FileInputStream[] in2Arr = new FileInputStream[t2SublistsCount];

            //t1
            int[] lengthOfT1Buffer = new int[t1SublistsCount];
            int[] currentT1 = new int[t1SublistsCount];

            //t2
            int[] lengthOfT2Buffer = new int[t2SublistsCount];
            int[] currentT2 = new int[t2SublistsCount];

            //init and first read
            for (int i = 0; i < t1SublistsCount; i++) {
                in1Arr[i] = new FileInputStream(t1Prefix + "_" + i + ".txt");
                lengthOfT1Buffer[i] = loadFile(t1Buffer[i], in1Arr[i], 101);
                currentT1[i] = 0;
            }
            for (int i = 0; i < t2SublistsCount; i++) {
                in2Arr[i] = new FileInputStream(t2Prefix + "_" + i + ".txt");
                lengthOfT2Buffer[i] = loadFile(t2Buffer[i], in2Arr[i], 28);
                currentT2[i] = 0;
            }


            while (true) {
                //get smallest id in T1
                byte[] miniT1 = new byte[101];
                Arrays.fill(miniT1, Byte.MAX_VALUE);

                for (int i = 0; i < t1SublistsCount; i++) {
                    if (compare(t1Buffer[i][currentT1[i]], miniT1) < 0) {
                        miniT1 = t1Buffer[i][currentT1[i]].clone();           
                    }
                }


                //get smallest id in T2
                byte[] miniT2 = new byte[27];
                Arrays.fill(miniT2,Byte.MAX_VALUE);

                for (int i = 0; i < t2SublistsCount; i++) {
                    if (compare(t2Buffer[i][currentT2[i]], miniT2) < 0) {
                        miniT2 = t2Buffer[i][currentT2[i]].clone();
                    }
                }

                //compare between T1 and T2
                if (compare(miniT1, miniT2) < 0) {     // the student with miniT1 have not finished any course
                    //pass all records about this student
                    for (int i = 0; i < t1SublistsCount; i++) {
                        while (compare(t1Buffer[i][currentT1[i]], miniT1) == 0) {
                            currentT1[i]++;
                            //if reach bottom of buffer, load file
                            if (currentT1[i] == lengthOfT1Buffer[i]) {
                                lengthOfT1Buffer[i] = loadFile(t1Buffer[i], in1Arr[i], 101);
                                currentT1[i] = 0;
                                //trick
                                if (lengthOfT1Buffer[i] < 40) {     //means last block
                                    byte[] biggest = new byte[100];
                                    Arrays.fill(biggest, Byte.MAX_VALUE);
                                    t1Buffer[i][lengthOfT1Buffer[i]] = biggest;
                                    lengthOfT1Buffer[i] ++;
                                }

                            }

                            //write student ID with GPA 0
                            byte[] studentId = new byte[8];
                            System.arraycopy(miniT1, 0, studentId, 0, 8);
                            outputPointer = writeFile(outputBuffer, outputPointer, studentId, 0.0f, out);
                        }
                    }
                }
                else if (compare(miniT1, miniT2) > 0) {       //invalid student
                    for (int i = 0; i < t2SublistsCount; i++) {
                        while (compare(miniT2, t2Buffer[i][currentT2[i]]) == 0) {
                            currentT2[i]++;

                            //if reach bottom, load file
                            if (currentT2[i] == lengthOfT2Buffer[i]) {
                                lengthOfT2Buffer[i] = loadFile(t2Buffer[i], in2Arr[i], 28);
                                currentT2[i] = 0;
                                //trick
                                if (lengthOfT2Buffer[i] < 130) {     //means last block
                                    byte[] biggest = new byte[28];
                                    Arrays.fill(biggest, Byte.MAX_VALUE);
                                    t2Buffer[i][lengthOfT2Buffer[i]] = biggest;
                                    lengthOfT2Buffer[i]++;
                                }
                            }
                        }

                    }
                }
                else{      // smallest T1 == smallest T2
                    if (isMaxByte(miniT1)) {
                        System.out.println("=========================== END  ===========================");
                        //write final output buffer to file
                        out.write(outputBuffer, 0, outputPointer);
                    
                        out2.write(outputBuffer2, 0, outputPointer2);
                        iocost++;
                        break;
                    }

                    for (int i = 0; i < t1SublistsCount; i++) {
                        //pass all duplicated record in T1
                        while (compare(t1Buffer[i][currentT1[i]], miniT1) == 0) {
                            currentT1[i]++;
                            //if reach bottom of buffer, load file
                            if (currentT1[i] == lengthOfT1Buffer[i]) {
                                lengthOfT1Buffer[i] = loadFile(t1Buffer[i], in1Arr[i], 101);
                                currentT1[i] = 0;
                                //trick
                                if (lengthOfT1Buffer[i] < 40) {     //means last block
                                    byte[] biggest = new byte[100];
                                    Arrays.fill(biggest, Byte.MAX_VALUE);
                                    t1Buffer[i][lengthOfT1Buffer[i]] = biggest;
                                    lengthOfT1Buffer[i] ++;
                                }
                            }
                        }
                    }

                    // accumulate values in T2
                    int creditAccumulator = 0;
                    float creditGradeAccumulator = 0.0f;
                    for (int i = 0; i < t2SublistsCount; i++) {
                        while (compare(miniT2, t2Buffer[i][currentT2[i]]) == 0) {
                            byte[] creditByte = new byte[2];
                            byte[] gradeByte = new byte[4];
                            System.arraycopy(t2Buffer[i][currentT2[i]], 21, creditByte, 0, 2);
                            System.arraycopy(t2Buffer[i][currentT2[i]], 23, gradeByte, 0, 4);
                            int credit = Integer.parseInt(new String(creditByte).trim());
                            String creditStr = new String(gradeByte);
                            float grade = castGradeToVal(creditStr.trim());
                            
                            
                            byte[] temptt1_arr = new byte[100];
                            System.arraycopy(miniT1,0,temptt1_arr,0,100);
                            
                            byte[] temptt2_arr = new byte[27];
                            System.arraycopy( t2Buffer[i][currentT2[i]],0,temptt2_arr,0,27);
                            
                            String tempTT1 = new String(temptt1_arr);
                            String tempTT2 = new String(temptt2_arr);
                            //todo
                            outputPointer2 = writeFile2(outputBuffer2, outputPointer2, tempTT1,tempTT2, out2);
                            
                            
                            creditAccumulator += credit;
                            creditGradeAccumulator += (grade * credit);

                            currentT2[i]++;

                            //if reach bottom, load file
                            if (currentT2[i] == lengthOfT2Buffer[i]) {
                                lengthOfT2Buffer[i] = loadFile(t2Buffer[i], in2Arr[i], 28);
                                currentT2[i] = 0;
                                //trick
                                if (lengthOfT2Buffer[i] < 130) {     //means last block
                                    byte[] biggest = new byte[28];
                                    Arrays.fill(biggest, Byte.MAX_VALUE);
                                    t2Buffer[i][lengthOfT2Buffer[i]] = biggest;
                                    lengthOfT2Buffer[i]++;
                                }
                            }
                        }
                    }

                    //write final GPA of the student
                    float avgGpa = creditGradeAccumulator / creditAccumulator;
                    byte[] studentId = new byte[8];
                    System.arraycopy(miniT1, 0, studentId, 0, 8);
                    outputPointer = writeFile(outputBuffer, outputPointer, studentId, avgGpa, out);
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public static void recordTime(long startTime, int divide, String function) {
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        System.out.println(function + " time is:" + duration / divide + " milliseconds");
    }

    public static void recordTime(long startTime, String function) {
        recordTime(startTime, 1, function);
    }

    public static void recordMemory(float start, String funciont) {
        float used = start - Runtime.getRuntime().freeMemory() / K;
        System.out.println("Memory used:" + used + "KB");
        System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
        System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (K * K) + "MB\n");
    }


    /**
     * compare the diff between ID of two tuple
     */
    public static int compare(byte[] t1, byte[] t2) {
        for (int i = 0; i < 8; i++) {
            if (t1[i] == t2[i]) {
                continue;
            } else {
                return t1[i] - t2[i];
            }
        }

        return 0;
    }


    public static boolean isMaxByte(byte[] byt) {
        boolean flag = true;
        for (byte b : byt) {
            if (b != Byte.MAX_VALUE) {
                flag = false;
                break;
            }
        }
        return flag;
    }


    /**
     * return the length of tuple in memory buffer, return 0 if end of file
     */
    public static int loadFile(byte[][] buffer, FileInputStream ios, int bytesOfTuple) throws IOException {
        byte[] blockBuffer;

        if (bytesOfTuple == 101) {
            blockBuffer = new byte[40 * 101];
        } else {
            blockBuffer = new byte[130 * 28];
        }

        int readLength;
        int lineOfBuffer = 0;
        //reading
        readLength = ios.read(blockBuffer);
        int i = (readLength / bytesOfTuple);

        for (int t = 0; t < i; t++) {
            System.arraycopy(blockBuffer, bytesOfTuple * t, buffer[lineOfBuffer++], 0, bytesOfTuple);
        }
        iocost++;
        return i;
    }


    public static int writeFile(byte[] outputBuffer, int outputPointer, byte[] studentIdBytes, float gpa, FileOutputStream out) {
        String studentId = new String(studentIdBytes);
        String temp = studentId + " " + String.valueOf(gpa) + "\r\n";
        byte[] tempByte = temp.getBytes();

        if (outputBuffer.length - outputPointer - 1 < tempByte.length) {
            try {
                out.write(outputBuffer, 0, outputPointer);
                iocost++;
                outputPointer = 0;
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }


        //copy data
        System.arraycopy(tempByte, 0, outputBuffer, outputPointer, tempByte.length);
        outputPointer += tempByte.length;
        return outputPointer;
    }
    
    
    
    
    public static int writeFile2(byte[] outputBuffer, int outputPointer, String t1, String t2, FileOutputStream out) {
        String temp = t1 + " " + t2 + "\r\n";
        byte[] tempByte = temp.getBytes();

        if (outputBuffer.length - outputPointer - 1 < tempByte.length) {
            try {
                out.write(outputBuffer, 0, outputPointer);
                iocost++;
                outputPointer = 0;
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }


        //copy data
        System.arraycopy(tempByte, 0, outputBuffer, outputPointer, tempByte.length);
        outputPointer += tempByte.length;
        return outputPointer;
    }
    


    public static float castGradeToVal(String grade) {
        switch (grade) {
            case "A+":
                return 4.3f;
            case "A":
                return 4.0f;
            case "A-":
                return 3.7f;
            case "B+":
                return 3.3f;
            case "B":
                return 3f;
            case "B-":
                return 2.7f;
            case "C+":
                return 2.3f;
            case "C":
                return 2f;
            case "C-":
                return 1.7f;
            case "D+":
                return 1.3f;
            case "D":
                return 1f;
            case "D-":
                return 0.7f;
            case "Fail":
                return 0;
            case "R":
                return 0;
            default:
                return 0;
        }
    }
}
