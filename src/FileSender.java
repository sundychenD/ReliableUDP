import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

public class FileSender {

    public static void main(String[] args) {

        if (args.length != 4) {
            System.err.println("Usage: FileSender <host_name>, <port_number>, <source_file>, <destination_file_name>");
            System.exit(-1);
        }

        String hostName = args[0];
        int portNumber = Integer.parseInt(args[1]);
        String sourceFile = args[2];
        String destinationFileName = args[3];

        FileSenderEngine sender = new FileSenderEngine(hostName, portNumber, sourceFile, destinationFileName);
        sender.run();

    }
}

class FileSenderEngine {
    private final int PACKET_CONTENT_LEN = 512;
    private final int ACK_LEN = 12;
    private final int HEADER_LEN = 12;
    private final int WINDOW_LEN = 5;

    private final String hostName;
    private final int portNumber;
    private final String sourceFile;
    private final String destinationFileName;

    private InetSocketAddress address;
    private DatagramSocket UDPSocket;
    private File file;
    private InputStream sourceFileStream;

    public FileSenderEngine(String host_name,
                            int port_number,
                            String source_file,
                            String destination_file_name) {
        this.hostName = host_name;
        this.portNumber = port_number;
        this.sourceFile = source_file;
        this.destinationFileName = destination_file_name;
    }

    public void run() {

        try {
            this.address = new InetSocketAddress(this.hostName, this.portNumber);
            this.UDPSocket = new DatagramSocket();
            this.file = new File(this.sourceFile);
            this.sourceFileStream = new FileInputStream(this.file);

            long len = this.file.length();
            int numPacket = (int) Math.ceil((double) len / this.PACKET_CONTENT_LEN);

            // Plain Send
            plainSend(numPacket);

            // Close I/O Channel
            this.sourceFileStream.close();
            this.UDPSocket.close();

        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /*
    * Plain Send through UDP
    * Data Packet would be [checkSum 8 bytes, index 4 bytes, content up to 512 bytes]
    * */
    private void plainSend(int numPacket) throws IOException {
        DatagramPacket pkt;
        CRC32 crc = new CRC32();
        byte[] packetBuffer = new byte[this.HEADER_LEN + this.PACKET_CONTENT_LEN];

        int packetIndex = 1;
        while (packetIndex <= numPacket) {
            System.out.println("===== Sender processing packet -- " + packetIndex);
            pkt = formContentPacket(packetIndex, crc, packetBuffer);

            // Keep sending until receive ack
            this.UDPSocket.send(pkt);
            System.out.println(" ===== Packet Send -- " + packetIndex);
            while (notReceiveAck(packetIndex)) {
                System.out.println(" ===== No ACT, Packet resend -- " + packetIndex);
                this.UDPSocket.send(pkt);
            }

            // Update next packet index
            packetIndex++;
        }
    }

    /*
    * Judge if receive valid acknowledge from receiver
    *
    * 3 Cases considered as invalid
    *   - timeout
    *   - checksum not match, packet corrupted
    *   - incorrect packet index
    * */
    private boolean notReceiveAck(int packetIndex) throws IOException {

        byte[] ackByteBuffer = new byte[this.ACK_LEN];
        DatagramPacket pkt = new DatagramPacket(ackByteBuffer, ackByteBuffer.length);
        ByteBuffer byteBuffer = ByteBuffer.wrap(ackByteBuffer);
        CRC32 crc = new CRC32();

        long stopTime = System.currentTimeMillis()+ TimeUnit.SECONDS.toMillis(200);

        while (System.currentTimeMillis() < stopTime) {
            System.out.println(" * Ack not time out -- " + packetIndex);
            pkt.setLength(ackByteBuffer.length);
            this.UDPSocket.receive(pkt);
            if (pkt.getLength() < 8) {
                System.out.println(" ===== ACK too short");
                continue;
            }

            byteBuffer.rewind();
            long chksum = byteBuffer.getLong();
            int ackIndex = byteBuffer.getInt();
            crc.reset();
            crc.update(ackByteBuffer, 8, pkt.getLength() - 8);

            // Debug output
            // System.out.println("Received CRC:" + crc.getValue() + " Data:" + bytesToHex(data, pkt.getLength()));

            if (crc.getValue() == chksum) {
                if (ackIndex == packetIndex) {
                    System.out.println("===== ACK received -- " + ackIndex);
                    return false;
                } else {
                    System.out.println(" ===== ACK order wrong -- " + ackIndex + " expected -- " + packetIndex);
                    return true;
                }
            } else {
                System.out.println(" ===== ACK corrupted -- " + ackIndex);
                return true;
            }
        }

        System.out.println(" ===== Timeout 200 ms, no ACK -- " + packetIndex);
        return true;
    }

    /*
    * Form datagram packet
    * */
    private DatagramPacket formContentPacket(int packetIndex,
                                             CRC32 crc,
                                             byte[] packetBuffer) throws IOException {
        // Read From Source File
        int contentLen;
        contentLen = this.sourceFileStream.read(packetBuffer, this.HEADER_LEN, this.PACKET_CONTENT_LEN);
        ByteBuffer b = ByteBuffer.wrap(packetBuffer);
        b.clear();

        // Reserve space for checksum
        b.putLong(0);
        b.putInt(packetIndex);

        // Calculate checksum
        crc.reset();
        crc.update(packetBuffer, 8, 4 + contentLen);
        long chksum = crc.getValue();

        b.rewind();
        b.putLong(chksum);

        return new DatagramPacket(packetBuffer, this.HEADER_LEN + contentLen, address);
    }

    final protected char[] hexArray = "0123456789ABCDEF".toCharArray();
    private String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

}
