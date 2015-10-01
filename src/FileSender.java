import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class FileSender {

    private static final int PACKET_CONTENT_LEN = 512;
    private static final int HEADER_LEN = 12;

    public static void main(String[] args) {

        if (args.length != 4) {
            System.err.println("Usage: FileSender <host_name>, <port_number>, <source_file>, <destination_file_name>");
            System.exit(-1);
        }

        String hostName = args[0];
        int portNumber = Integer.parseInt(args[1]);
        String sourceFile = args[2];
        String destinationFileName = args[3];

        try {
            InetSocketAddress address = new InetSocketAddress(hostName, portNumber);
            DatagramSocket UDPSocket = new DatagramSocket();
            InputStream sourceFileStream = getInputFile(sourceFile);

            // Plain Send
            plainSend(sourceFileStream, address, UDPSocket);

            // Close I/O Channel
            sourceFileStream.close();
            UDPSocket.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /*
    * Plain Send through UDP
    * Data Packet would be [checkSum 8 bytes, index 4 bytes, content up to 512 bytes]
    * */
    private static void plainSend(InputStream sourceFileStream,
                                  InetSocketAddress address,
                                  DatagramSocket socket) throws IOException {

        DatagramPacket pkt;
        ByteBuffer b;
        CRC32 crc = new CRC32();
        byte[] packetBuffer = new byte[HEADER_LEN + PACKET_CONTENT_LEN];

        // Read From Source File
        int contentLen;
        int packetIndex = 0;
        while ((contentLen = sourceFileStream.read(packetBuffer, HEADER_LEN, PACKET_CONTENT_LEN)) != -1) {
            b = ByteBuffer.wrap(packetBuffer);

            b.clear();
            // Reserve space for checksum
            b.putLong(0);
            b.putInt(packetIndex);

            // Calculate checksum
            crc.reset();
            crc.update(packetBuffer, 8, contentLen);
            long chksum = crc.getValue();

            b.rewind();
            b.putLong(chksum);

            // Form data packet and send
            pkt = new DatagramPacket(packetBuffer, HEADER_LEN + contentLen, address);
            socket.send(pkt);

            // Update packet index value
            packetIndex++;
        }
    }

    /*
    * Read Input File into stream
    * */
    private static InputStream getInputFile(String sourceFile) throws FileNotFoundException {
        File file = new File(sourceFile);
        InputStream srcFileStream = new FileInputStream(file);
        return srcFileStream;
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
