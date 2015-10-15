import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.zip.CRC32;

/**
 * Created by chendi on 1/10/15.
 *
 *
 *
 * For initial meta data packet
 * [8 checksum, 4 index, 4 num of packets, up to 508 byte file name]
 *
 *
 * For normal data packet
 * [8 checksum, 4 index, up to 512 byte file name]
 */
public class FileReceiver {

    private static DatagramSocket sk;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: FileReceiver <host_name>");
            System.exit(-1);
        }

        int port = Integer.parseInt(args[0]);
        try {
            sk = new DatagramSocket(port);
            FileReceiverEngine receiverEngine = new FileReceiverEngine(sk);
            receiverEngine.run();

        } catch (SocketException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

class FileReceiverEngine {

    private final DatagramSocket socket;
    private final int PKT_CORRUPTED_ACK = -2;

    public FileReceiverEngine(DatagramSocket sk) {
        this.socket = sk;
    }

    public void run() throws IOException {
        // Get file meta data
        Pair fileMetaData = receiveFileMetaData();
        String fileName = fileMetaData.getValue();
        int numOfPackets = fileMetaData.getKey();

        // Receive file
        // continueReceiveFile(fileName, numOfPackets);
        receiveFile(fileName, numOfPackets);
    }

    /*
    * Get File meta data
    * */
    private Pair receiveFileMetaData() throws IOException {
        String fileName = null;
        int numOfPackets = 0;
        while (true) {
            FilePacket filePacket = new FilePacket(this.socket);
            filePacket.receivePkt();

            if (!filePacket.isCorrupted()) {
                if (filePacket.isMetaPackage()) {
                    // Get Meta packet, abandon it and send ACK -1
                    sendACK(-1, filePacket.getSocketAddress());
                    fileName = filePacket.getFileName();
                    numOfPackets = filePacket.getNumOfPackets();
                } else {
                    // Sender received meta ACK
                    // Cur packet is the first content packet, abandon it and send ACK -1
                    // Break meta packet check loop, go to content file receiving loop
                    sendACK(-1, filePacket.getSocketAddress());
                    break;
                }
            } else {
                // Meta packet corrupted
                sendACK(this.PKT_CORRUPTED_ACK, filePacket.getSocketAddress());
            }
        }
        return new Pair (numOfPackets, fileName);
    }

    private void receiveFile(String fileName, int totalNumPacket) throws IOException{
        // Output file
        File file = new File(fileName);
        OutputStream outputStream = new FileOutputStream(file);

        int[] receivedPacketList = new int[totalNumPacket];
        LinkedList<FilePacket> packetBuffer = new LinkedList<FilePacket>();
        int receivedPacketTill = 0;
        int packetIndex = 0;
        while (true) {

            // Receive packet from socket
            FilePacket filePacket = new FilePacket(this.socket);
            filePacket.receivePkt();

            if (filePacket.pkt.getLength() < 8) {
                System.out.println(" ===== Pkt too short");
                continue;
            }
            packetIndex = filePacket.getPacketIndex();

            // Debug output
            // System.out.println("Received CRC:" + crc.getValue() + " Data:" + bytesToHex(data, pkt.getLength()));

            // Send Acknowledgement
            if (filePacket.isCorrupted()) {
                //System.out.println(" ===== Pkt corrupt -- " + packetIndex);
                sendACK(this.PKT_CORRUPTED_ACK, filePacket.getSocketAddress());
            } else {
                if (packetIndex == receivedPacketTill) {
                    // Receive in order packet
                    //System.out.println("===== SUCCESS! Pkt received -- " + packetIndex);
                    sendACK(packetIndex, filePacket.getSocketAddress());

                    receivedPacketList[packetIndex] = 1;
                    packetBuffer.add(filePacket);
                    //System.out.println("Add packet to buffer -- " + filePacket.getPacketIndex());
                    int newlyWritePackets = writeFromBuffer(receivedPacketTill,
                                                            receivedPacketList,
                                                            packetBuffer,
                                                            outputStream);
                    //printPacketBuffer(packetBuffer);
                    receivedPacketTill += newlyWritePackets;
                } else {
                    // Packet out of order
                    if (receivedPacketList[packetIndex] == 1) {
                        // Already received packet
                        //System.out.println(" ===== SUCCESS! Pkt already have, abandon -- " + packetIndex);
                    } else {
                        //System.out.println(" ===== SUCCESS! Pkt out of order, store in buffer -- " + packetIndex);
                        receivedPacketList[packetIndex] = 1;
                        packetBuffer.add(filePacket);
                        //System.out.println("Add packet to buffer -- " + filePacket.getPacketIndex());
                        //printPacketBuffer(packetBuffer);
                    }
                    sendACK(packetIndex, filePacket.getSocketAddress());
                }
            }

            // Close if received full length of packet
            if (receivedPacketTill == totalNumPacket) {
                //System.out.println(" ===== Close output file " + fileName);
                outputStream.close();
            }
        }
    }

    /*
    * Pop packet from buffer and write to disc.
    * */
    private int writeFromBuffer (int receivedPacketTill,
                                 int[] receivedPacketList,
                                 LinkedList<FilePacket> packetBuffer,
                                 OutputStream outputStream) throws IOException {
        int totalPktWriteToDisc = 0;
        int curPacketIndex = receivedPacketTill;

        // Search through the buffer
        while (curPacketIndex < receivedPacketList.length && receivedPacketList[curPacketIndex] == 1) {
            FilePacket filePacket = popPktFromBuffer(packetBuffer, curPacketIndex);
            long bytesWrite = filePacket.getDataLength() - 12;
            //System.out.println("===== Write " + bytesWrite + "bytes, packet index: " + filePacket.getPacketIndex() + " buffered index: " + curPacketIndex);

            outputStream.write(filePacket.data, 12, filePacket.getDataLength() - 12);
            totalPktWriteToDisc += 1;
            curPacketIndex += 1;
        }
        //printPacketBuffer(packetBuffer);
        return totalPktWriteToDisc;
    }

    private void printPacketBuffer(LinkedList<FilePacket> packetBuffer) {
        System.out.println("*******************");
        FilePacket curPacket = null;
        for (int i = 0; i < packetBuffer.size(); i++) {
            curPacket = packetBuffer.get(i);
            System.out.println("* i -- " + i + " packet index -- " + curPacket.getPacketIndex());
        }
        System.out.println("*******************");
    }

    /*
    * Find the packet with corresponding index from buffer
    * */
    private FilePacket popPktFromBuffer(LinkedList<FilePacket> packetBuffer, int index) {
        FilePacket curPacket = null;
        for (int i = 0; i < packetBuffer.size(); i++) {
            curPacket = packetBuffer.get(i);
            //System.out.println("buffer cur index is -- " + curPacket.getPacketIndex() + "  required index is -- " + index + " -- i " + i);
            if (curPacket.getPacketIndex() == index) {
                packetBuffer.remove(i);
                //System.out.println("===== Pop packet from buffer at index -- " + i + " packet index -- " + curPacket.getPacketIndex());
                return curPacket;
            }
        }
        return null;
    }

    /*
    * Linear receive file
    * */
    private void continueReceiveFile(String fileName, int numPacket) {
        try {
            // Output file
            File file = new File(fileName);
            OutputStream outputStream = new FileOutputStream(file);

            FilePacket filePacket = new FilePacket(this.socket);;

            int receivedPacket = 0;
            int packetIndex = 0;
            while (true) {

                // Receive packet from socket
                filePacket.receivePkt();

                if (filePacket.pkt.getLength() < 8) {
                    System.out.println(" ===== Pkt too short");
                    continue;
                }
                packetIndex = filePacket.getPacketIndex();

                // Debug output
                // System.out.println("Received CRC:" + crc.getValue() + " Data:" + bytesToHex(data, pkt.getLength()));

                // Send Acknowledgement
                if (filePacket.isCorrupted()) {
                    System.out.println(" ===== Pkt corrupt -- " + packetIndex);
                    sendACK(receivedPacket, filePacket.getSocketAddress());
                } else {
                    if (packetIndex == (receivedPacket + 1)) {
                        // Receive correct packet
                        System.out.println("===== SUCCESS! Pkt received -- " + packetIndex);
                        sendACK(packetIndex, filePacket.getSocketAddress());

                        long bytesWrite = filePacket.getDataLength() - 12;
                        System.out.println("===== Write " + bytesWrite + "bytes");
                        outputStream.write(filePacket.data, 12, filePacket.getDataLength() - 12);

                        receivedPacket += 1;
                    } else {
                        // Packet out of order
                        System.out.println(" ===== Pkt out of order -- " + packetIndex);
                        sendACK(receivedPacket, filePacket.getSocketAddress());
                    }
                }

                // Close if received full length of packet
                if (packetIndex == numPacket) {
                    outputStream.close();
                }
            }

        } catch (SocketException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void sendACK(int packetIndex, SocketAddress address) throws IOException {
        CRC32 crc = new CRC32();

        byte[] ack = new byte[12];
        ByteBuffer ackBuffer = ByteBuffer.wrap(ack);
        ackBuffer.clear();

        // Reserve space for checksum
        ackBuffer.putLong(0);
        ackBuffer.putInt(packetIndex);

        // Calculate checksum
        crc.reset();
        crc.update(ack, 8, 4);
        long ackCheckSum = crc.getValue();

        ackBuffer.rewind();
        ackBuffer.putLong(ackCheckSum);

        DatagramPacket ackPacket = new DatagramPacket(ack, 0, 12, address);
        this.socket.send(ackPacket);
    }
}

class FilePacket {

    private final int PACKET_LEN = 524;      // 12 byte header + up to 512 byte content
    public byte[] data;
    public ByteBuffer byteBuffer;
    public DatagramPacket pkt;
    private DatagramSocket socket;

    public FilePacket(DatagramSocket skt) {
        this.data = new byte[PACKET_LEN];
        this.byteBuffer = ByteBuffer.wrap(this.data);
        this.pkt = new DatagramPacket(this.data, this.data.length);
        this.pkt.setLength(this.data.length);
        this.socket = skt;
    }

    public void receivePkt() throws IOException {
        this.socket.receive(pkt);
    }

    public int getPacketIndex() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        return this.byteBuffer.getInt();
    }

    public int getDataLength() {
        return this.pkt.getLength();
    }

    public SocketAddress getSocketAddress() {
        return this.pkt.getSocketAddress();
    }

    public boolean isCorrupted() {
        this.byteBuffer.rewind();
        long checksum = this.byteBuffer.getLong();

        CRC32 crc = new CRC32();
        crc.reset();
        crc.update(this.data, 8, this.pkt.getLength() - 8);
        return crc.getValue() != checksum;
    }

    public boolean isMetaPackage() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        return this.byteBuffer.getInt() == -1;
    }

    /*
    * Method only used on meta packets
    *
    * return num of packets meta data
    * */
    public int getNumOfPackets() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        this.byteBuffer.getInt();       // Skip index flag
        return this.byteBuffer.getInt();
    }

    /*
    * Method only used on meta packets
    *
    * return filename meta data
    * */
    public String getFileName() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        this.byteBuffer.getInt();       // Skip index flag
        this.byteBuffer.getInt();       // Skip num of packets

        int fileNameCharLength = (this.pkt.getLength() - 16) / 2;
        String fileName = "";

        for (int i = 0; i < fileNameCharLength; i ++) {
            fileName += byteBuffer.getChar();
        }
        return fileName;
    }
}

class Pair {
    private String value;
    private int key;

    public Pair(int key, String value) {
        this.value = value;
        this.key = key;
    }

    public String getValue() {
        return this.value;
    }

    public int getKey() {
        return this.key;
    }
}
