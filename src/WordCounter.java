import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by gbenhaim on 4/12/16.
 */
public class WordCounter implements Runnable {

    private final int EXCEPTION_TRESHOLD = 3;
    private final char[] TO_AVOID = {'\''};

    private int m_Count;
    private File m_File;
    private long m_Offset;
    private long m_Size;
    private int m_BufferSize;
    private RandomAccessFile m_RandomFile;
    private byte m_LastChar = 0;

    private byte[] m_Buffer;


    public static void main (String[] args) {


        String filePath = "WarAndPeace.txt";
        int bufferSize = 8912;
        int numOfThreads = 32;

        File file = new File(filePath);
        long fileSize = file.length();
        WordCounter[] wordCounters = new WordCounter[numOfThreads];
        Thread[] threads = new Thread[numOfThreads];

        long chunk = fileSize / numOfThreads;
        long reminder = fileSize % numOfThreads;

        // init Threads and WordCounters
        for (int i = 0; i < numOfThreads; i++) {

            if (i + 1 != numOfThreads) {
                wordCounters[i] = new WordCounter(file, i * chunk, chunk, bufferSize);

            } else {
                wordCounters[i]  = new WordCounter(file, i * chunk, chunk + reminder, bufferSize);
            }

            threads[i] = new Thread(wordCounters[i]);
        }

        long startTime = System.currentTimeMillis();

        // Starts the threads
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        // makes the thread of main to wait for the workers.
        for (int i = 0; i < threads.length; i++) {
            try {

                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.printf("The number of words is: %d\n", collectAndAdd(wordCounters));
        System.out.printf("Time: %.2f", (System.currentTimeMillis() - startTime) / 1000f);

    }

    /**
     *
     * @param wordCounters
     * @return The total number of words counted by all the WordCounters
     */
    public static int collectAndAdd(WordCounter[] wordCounters) {
        int counter = 0;
        for (WordCounter wc : wordCounters) {
            counter += wc.getCount();
        }
        return counter;
    }

    public WordCounter(File i_File, long i_Offset, long i_Size, int i_BufferSize) {
        m_File = i_File;
        m_Offset = i_Offset;
        m_Size = i_Size;
        m_BufferSize = i_BufferSize;
        m_Buffer = new byte[i_BufferSize];
    }


    @Override
    public void run() {

        printThreadMsg("Started");

        if (openRandomFile(m_File, m_Offset)) {

            long numOfBytesToRead = m_Size - 1; // -1 because checkFirstChar move the cursor one place forward
            int exceptionCount = 0; // We counting the number of failures in order to avoid infinite loop
            int bufferSize = 0;

            m_Count = checkFirstChar(); // see checkFirstChar doc

            while (numOfBytesToRead > 0 && exceptionCount < EXCEPTION_TRESHOLD) {

                try {
                    bufferSize = calcBufferSize(numOfBytesToRead, m_BufferSize);
                    read(m_RandomFile, bufferSize);
                    numOfBytesToRead = numOfBytesToRead - m_BufferSize;
                    m_Count += countWords(m_Buffer, bufferSize);

                } catch (IOException e) {
                    e.printStackTrace();
                    printThreadMsg("Error on reading file");
                    exceptionCount++;
                }
            }

            if (exceptionCount + 1 == EXCEPTION_TRESHOLD) {
                System.out.println("Failed with exception limit break");
            }

            try {
                m_Count += checkLastChar(m_RandomFile, m_Size, m_Offset); // see check last doc
                m_RandomFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                printThreadMsg("Finished");
            }
        }


    }

    private int calcBufferSize(long i_NumOfBytesToRead, int i_BufferSize) {

        int bufferSize = 0;

        if (i_NumOfBytesToRead < i_BufferSize) {
            bufferSize = (int) i_NumOfBytesToRead;
        } else {
            bufferSize = i_BufferSize;
        }

        return bufferSize;
    }

    /**
     * Reads i_BufferSize bytes from i_RandomFile into an array of bytes
     * @param i_RandomFile
     * @param i_BufferSize
     * @return array of bytes with the data the was read
     */
    private void read(RandomAccessFile i_RandomFile, int i_BufferSize) throws IOException {


        // We want to avoid trailing zeros in the buffer array,
        // this situation could only occur when the buffer size is bigger then
        // the number of bytes left to read in the file.

        try {
           // buffer = new byte[bufferSize];
            i_RandomFile.read(m_Buffer, 0, i_BufferSize);
        } catch (EOFException e) {
            System.err.println(e);
        }


    }

    /**
     * init m_RandomFile with m_File
     * @param i_File
     * @param i_Offset offset from the start of the file
     * @return true if the operation succeed
     */
    private boolean openRandomFile(File i_File, long i_Offset) {
        try {
            m_RandomFile = new RandomAccessFile(i_File, "r");
            m_RandomFile.seek(i_Offset);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            printThreadMsg("Error on open file");
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.printf("Couldn't seek to: %l. thread: %s\n",
                            i_Offset, Thread.currentThread().getName());
        }

        return true;
    }

    /**
     * this method is used for smooth transition between the parts of
     * the file that were divided to the threads.
     * The idea is that each word is counted if it end with an invalid char, and the char
     * before it should be valid.
     * this method is bridging on the gap that was created when dividing a word for the process of
     * two threads.
     *
     * @return 1 if the first letter on this thread section is invalid and the last letter from
     * the previous thread is valid.
     */
    private int checkFirstChar() {
        try {
            if (m_Offset > 0) {
                m_RandomFile.seek(m_RandomFile.getFilePointer() - 1);
                byte fromLastThread = m_RandomFile.readByte();
                byte current = m_RandomFile.readByte();
                m_LastChar = current;
                if (isValidChar(fromLastThread) && !isValidChar(current)) {
                    return 1;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * if the chunk is not ending with an invalid letter, the last word won't be
     * counted. this method will fix this issue.
     * @param i_RandomFile
     * @param i_Size
     * @param i_Offset
     * @return 1 if the last character of the last chunk is valid.
     */

    private int checkLastChar (RandomAccessFile i_RandomFile, long i_Size, long i_Offset) {

        if (i_Size + i_Offset == m_File.length()) {
            try {
                i_RandomFile.seek(i_Size + i_Offset - 1);
                return isValidChar(i_RandomFile.readByte()) ? 1 : 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }


    /**
     *
     * @param i_Buffer
     * @return The number of words in a given buffer
     */

    private long countWords(byte[] i_Buffer, int i_BufferSize) {

        int counter = 0;

        for (int i = 0; i < i_BufferSize; i++) {

            if (!isCharToAvoid(i_Buffer[i]) &&
                !isValidChar(i_Buffer[i]) &&
                isValidChar(m_LastChar)) {

                counter++;
            }

            m_LastChar = i_Buffer[i];

        }

        return counter;
    }

    /**
     * chars to avoid are chars that are not valid but also don't separate between words,
     * he's - ' for example
     * @param i_Char
     * @return true if we should avoid from i_Char, false otherwise
     */

    private boolean isCharToAvoid(byte i_Char) {

        for (char c : TO_AVOID) {
            if (c == i_Char) {
                return true;
            }
        }

        return false;
    }

    /**
     *
     * @param i_Char
     * @return true if i_Char belong to [A..Z] or [a..z], false otherwise
     */

    private boolean isValidChar(byte i_Char) {

        boolean b =((i_Char > 64 && i_Char < 91) || (i_Char > 96 && i_Char < 123));
        return b;
    }

    /**
     *
     * @return the number of words that this WordCounter counted
     */

    public int getCount() {
        return m_Count;
    }

    /**
     * Prints a massage to the console with an addition of the name of the current thread
     * @param msg the message to print
     */
    private void printThreadMsg(String msg) {
        System.out.printf("%s - %s\n", Thread.currentThread().getName(), msg);
    }
}
